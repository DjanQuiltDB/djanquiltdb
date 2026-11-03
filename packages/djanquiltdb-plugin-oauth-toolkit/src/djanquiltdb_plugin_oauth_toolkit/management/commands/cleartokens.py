from django.core.management import CommandError
from oauth2_provider.management.commands.cleartokens import Command as ToolkitCleartokensCommand

from djanquiltdb_plugin_oauth_toolkit.utils import clear_expired_for_all_shards


class Command(ToolkitCleartokensCommand):
    """
    Replaces the toolkit's ``cleartokens`` command when ``djanquiltdb_plugin_oauth_toolkit`` comes before
    ``oauth2_provider`` in ``INSTALLED_APPS``.
    """

    help = (
        "Runs django-oauth-toolkit's clear_expired() in every active shard, which deletes the expired tokens and "
        'grants of that shard. A shard in maintenance is skipped, unless --ignore-maintenance is given.'
    )

    def add_arguments(self, parser):
        super().add_arguments(parser)

        parser.add_argument(
            '--ignore-maintenance',
            action='store_true',
            dest='ignore_maintenance',
            help=(
                'Also clear the expired tokens of the shards that are in maintenance. A move puts a shard in '
                "maintenance while it copies the shard's data, so wait for a running move to finish."
            ),
        )

    def handle(self, *args, **options):
        from oauth2_provider.models import refresh_token_expire_timedelta

        if refresh_token_expire_timedelta() is None:
            self.stderr.write(
                self.style.WARNING(
                    'REFRESH_TOKEN_EXPIRE_SECONDS is not set or is 0, so refresh tokens do not expire. Only revoked '
                    'refresh tokens and refresh tokens without an access token are deleted. An expired access token '
                    'or ID token that belongs to a refresh token is kept until that refresh token is deleted.'
                )
            )

        try:
            cleared_shards, skipped_shards = clear_expired_for_all_shards(
                ignore_maintenance=options['ignore_maintenance']
            )
        except ExceptionGroup as errors:
            # Each error of the group has a note that names its shard.
            raise CommandError(
                '\n'.join(
                    [errors.message] + ['{} {}'.format(' '.join(error.__notes__), error) for error in errors.exceptions]
                )
            ) from errors

        if options['verbosity'] >= 1:
            for shard in cleared_shards:
                self.stdout.write('Deleted the expired tokens of shard {}.'.format(shard.alias))

            for shard in skipped_shards:
                self.stdout.write(
                    'Skipped shard {}, which is in maintenance. Pass --ignore-maintenance to clear it.'.format(
                        shard.alias
                    )
                )
