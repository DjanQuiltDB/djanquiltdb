import json
import warnings
from collections import OrderedDict

import yaml
from django.apps import apps
from django.core import serializers
from django.core.management.base import CommandError
from django.core.management.commands.dumpdata import Command as DumpDataCommand
from django.core.management.commands.dumpdata import ProxyModelWarning
from django.core.management.utils import parse_apps_and_model_labels
from django.db import router

from djanquiltdb import ShardingMode, State
from djanquiltdb.utils import get_model_sharding_mode, get_shard_class, use_shard


class Command(DumpDataCommand):
    help = 'Schema-aware dumpdata that produces DjanquiltDB-compatible fixtures.'

    def handle(self, *app_labels, **options):
        format = options['format']
        if not format:
            format = 'json'
        indent = options['indent']
        using = options['database']
        excludes = options['exclude']
        output = options['output']
        show_traceback = options['traceback']
        use_natural_foreign_keys = options['use_natural_foreign_keys']
        use_natural_primary_keys = options['use_natural_primary_keys']
        use_base_manager = options['use_base_manager']
        pks = options['primary_keys']

        if pks:
            primary_keys = [pk.strip() for pk in pks.split(',')]
        else:
            primary_keys = []

        excluded_models, excluded_apps = parse_apps_and_model_labels(excludes)

        if not app_labels:
            if primary_keys:
                raise CommandError('You can only use --pks option with one model')
            app_list = dict.fromkeys(
                app_config
                for app_config in apps.get_app_configs()
                if app_config.models_module is not None and app_config not in excluded_apps
            )
        else:
            if len(app_labels) > 1 and primary_keys:
                raise CommandError('You can only use --pks option with one model')
            app_list = {}
            for label in app_labels:
                try:
                    app_label, model_label = label.split('.')
                    try:
                        app_config = apps.get_app_config(app_label)
                    except LookupError as e:
                        raise CommandError(str(e))
                    if app_config.models_module is None or app_config in excluded_apps:
                        continue
                    try:
                        model = app_config.get_model(model_label)
                    except LookupError:
                        raise CommandError('Unknown model: %s.%s' % (app_label, model_label))
                    app_list_value = app_list.setdefault(app_config, [])
                    if app_list_value is not None and model not in app_list_value:
                        app_list_value.append(model)
                except ValueError:
                    if primary_keys:
                        raise CommandError('You can only use --pks option with one model')
                    app_label = label
                    try:
                        app_config = apps.get_app_config(app_label)
                    except LookupError as e:
                        raise CommandError(str(e))
                    if app_config.models_module is None or app_config in excluded_apps:
                        continue
                    app_list[app_config] = None

        if format not in serializers.get_public_serializer_formats():
            try:
                serializers.get_serializer(format)
            except serializers.SerializerDoesNotExist:
                pass
            raise CommandError('Unknown serialization format: %s' % format)

        # Resolve models from app_list
        if use_natural_foreign_keys:
            models = serializers.sort_dependencies(app_list.items(), allow_cycles=True)
        else:
            models = []
            for app_config, model_list in app_list.items():
                if model_list is None:
                    models.extend(app_config.get_models())
                else:
                    models.extend(model_list)

        # Split into public-schema and sharded models, filtering as we go
        public_models = []
        sharded_models = []
        for model in models:
            if model in excluded_models:
                continue
            if model._meta.proxy and model._meta.proxy_for_model not in models:
                warnings.warn(
                    "%s is a proxy model and won't be serialized." % model._meta.label,
                    category=ProxyModelWarning,
                )
                continue
            if model._meta.proxy:
                continue
            sharding_mode = get_model_sharding_mode(model)
            if sharding_mode == ShardingMode.SHARDED:
                sharded_models.append(model)
            elif router.allow_migrate_model(using, model):
                public_models.append(model)

        # Collect all objects as Python dicts
        all_dicts = []

        serialize_kwargs = {
            'use_natural_foreign_keys': use_natural_foreign_keys,
            'use_natural_primary_keys': use_natural_primary_keys,
        }

        def queryset_for_model(model, is_sharded=False):
            if use_base_manager:
                objects = model._base_manager
            else:
                objects = model._default_manager
            if is_sharded:
                queryset = objects.order_by(model._meta.pk.name)
            else:
                queryset = objects.using(using).order_by(model._meta.pk.name)
            if primary_keys:
                queryset = queryset.filter(pk__in=primary_keys)
            return queryset

        # Serialize public-schema models (no _schema tag)
        for model in public_models:
            queryset = queryset_for_model(model)
            if not queryset.exists():
                continue
            dicts = serializers.serialize('python', queryset, **serialize_kwargs)
            all_dicts.extend(_order_keys(d) for d in dicts)

        # Serialize sharded models per active shard
        Shard = get_shard_class()
        shards = Shard.objects.filter(state=State.ACTIVE).order_by('alias')
        for shard in shards:
            with use_shard(shard):
                for model in sharded_models:
                    queryset = queryset_for_model(model, is_sharded=True)
                    if not queryset.exists():
                        continue
                    dicts = serializers.serialize('python', queryset, **serialize_kwargs)
                    for d in dicts:
                        d['_schema'] = shard.schema_name
                    all_dicts.extend(_order_keys(d) for d in dicts)

        # Write output
        try:
            self.stdout.ending = None
            if output:
                stream = self._open_output(output)
            else:
                stream = None

            output_stream = stream or self.stdout
            self._write_output(all_dicts, format, indent, output_stream)

            if stream:
                stream.close()
        except Exception as e:
            if show_traceback:
                raise
            raise CommandError('Unable to serialize database: %s' % e)

    def _open_output(self, output):
        """Open an output file, handling compression formats."""
        import gzip
        import os

        try:
            import bz2

            has_bz2 = True
        except ImportError:
            has_bz2 = False

        try:
            import lzma

            has_lzma = True
        except ImportError:
            has_lzma = False

        file_root, file_ext = os.path.splitext(output)
        compression_formats = {
            '.bz2': (open, {}, file_root),
            '.gz': (gzip.open, {}, output),
            '.lzma': (open, {}, file_root),
            '.xz': (open, {}, file_root),
            '.zip': (open, {}, file_root),
        }
        if has_bz2:
            compression_formats['.bz2'] = (bz2.open, {}, output)
        if has_lzma:
            compression_formats['.lzma'] = (lzma.open, {'format': lzma.FORMAT_ALONE}, output)
            compression_formats['.xz'] = (lzma.open, {}, output)
        try:
            open_method, kwargs, file_path = compression_formats[file_ext]
        except KeyError:
            open_method, kwargs, file_path = (open, {}, output)
        if file_path != output:
            file_name = os.path.basename(file_path)
            warnings.warn(
                f"Unsupported file extension ({file_ext}). Fixtures saved in '{file_name}'.",
                RuntimeWarning,
            )
        return open_method(file_path, 'wt', **kwargs)

    def _write_output(self, dicts, format, indent, stream):
        """Write the collected dicts to the stream in the target format."""
        if format == 'json':
            json.dump(dicts, stream, indent=indent, cls=_DjangoTypeEncoder)
            if indent:
                stream.write('\n')
        elif format == 'yaml':
            yaml.dump(
                dicts,
                stream,
                Dumper=_OrderedYamlDumper,
                default_flow_style=False,
                allow_unicode=True,
                indent=indent,
            )
        else:
            raise CommandError('Format %s is not supported for schema-aware dumpdata. Use json or yaml.' % format)


def _order_keys(d):
    """Re-order dict keys to fixture convention: model, pk, _schema, fields."""
    ordered = OrderedDict()
    ordered['model'] = d['model']
    if 'pk' in d:
        ordered['pk'] = d['pk']
    if '_schema' in d:
        ordered['_schema'] = d['_schema']
    ordered['fields'] = d['fields']
    return ordered


class _OrderedYamlDumper(yaml.SafeDumper):
    """YAML dumper that preserves OrderedDict key order."""

    pass


_OrderedYamlDumper.add_representer(
    OrderedDict,
    lambda dumper, data: dumper.represent_mapping(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data.items()),
)


class _DjangoTypeEncoder(json.JSONEncoder):
    """JSON encoder that handles types Django's serializer produces."""

    def default(self, obj):
        import datetime
        import decimal
        import uuid

        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, datetime.time):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return str(obj)
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        return super().default(obj)
