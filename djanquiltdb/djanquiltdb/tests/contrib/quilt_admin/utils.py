from unittest import mock

from django.test import SimpleTestCase

from djanquiltdb.contrib.quilt_admin.utils import CrossShardMappingUserProxy, CrossShardUserProxy


class _ShardActivationRecorder:
    """
    Stand-in for `use_shard` / `use_shard_for` that records every activation and tracks how deeply nested the
    (fake) shard context currently is, so tests can assert both the number of activations and that the context is
    active while an attribute is being resolved.
    """

    def __init__(self):
        self.calls = []
        self.active = 0

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        recorder = self

        class _Context:
            def __enter__(self):
                recorder.active += 1
                return self

            def __exit__(self, *exc_info):
                recorder.active -= 1
                return False

        return _Context()

    @property
    def call_count(self):
        return len(self.calls)


class _RecordingUser:
    """A wrapped user whose `name` attribute records whether the shard context was active when it was read."""

    def __init__(self, recorder):
        self._recorder = recorder
        self.active_during_name_access = None

    @property
    def name(self):
        self.active_during_name_access = self._recorder.active
        return 'Jon Snow'


class CrossShardUserProxyTests(SimpleTestCase):
    """Shard-id mode: the proxy is constructed with an already-resolved Shard object."""

    def setUp(self):
        self.shard = object()  # sentinel Shard object
        self.user = mock.Mock(is_staff=True, is_superuser=False, pk=7)
        self.recorder = _ShardActivationRecorder()
        patcher = mock.patch('djanquiltdb.contrib.quilt_admin.utils.use_shard', self.recorder)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_reads_attribute_value_from_wrapped_user(self):
        proxy = CrossShardUserProxy(self.user, self.shard)

        self.assertIs(proxy.is_staff, True)
        self.assertEqual(proxy.pk, 7)

    def test_repeated_access_of_same_attribute_activates_shard_once(self):
        proxy = CrossShardUserProxy(self.user, self.shard)

        for _ in range(200):
            self.assertIs(proxy.is_staff, True)

        self.assertEqual(self.recorder.call_count, 1)

    def test_distinct_attributes_activate_shard_once_each(self):
        proxy = CrossShardUserProxy(self.user, self.shard)

        for _ in range(50):
            proxy.is_staff
            proxy.is_superuser
            proxy.pk

        self.assertEqual(self.recorder.call_count, 3)

    def test_shard_context_is_active_during_attribute_resolution(self):
        user = _RecordingUser(self.recorder)
        proxy = CrossShardUserProxy(user, self.shard)

        self.assertEqual(proxy.name, 'Jon Snow')
        self.assertEqual(user.active_during_name_access, 1)
        self.assertEqual(self.recorder.active, 0)  # context exited afterwards

    def test_activation_uses_the_resolved_shard_object(self):
        proxy = CrossShardUserProxy(self.user, self.shard)

        proxy.is_staff

        (args, _kwargs) = self.recorder.calls[0]
        self.assertIs(args[0], self.shard)

    def test_dunder_access_bypasses_shard_activation(self):
        proxy = CrossShardUserProxy(self.user, self.shard)

        proxy.__class__
        repr(proxy._user)

        self.assertEqual(self.recorder.call_count, 0)

    def test_internal_user_attribute_is_not_proxied(self):
        proxy = CrossShardUserProxy(self.user, self.shard)

        self.assertIs(proxy._user, self.user)
        self.assertEqual(self.recorder.call_count, 0)

    def test_missing_attribute_raises_and_is_not_cached(self):
        user = object()  # has no `nope` attribute
        proxy = CrossShardUserProxy(user, self.shard)

        with self.assertRaises(AttributeError):
            proxy.nope

        self.assertNotIn('nope', proxy._attr_cache)
        self.assertFalse(hasattr(proxy, 'nope'))


class CrossShardMappingUserProxyTests(SimpleTestCase):
    """Mapping mode: the proxy is constructed with a mapping value to be resolved to a Shard once."""

    def setUp(self):
        self.shard = object()  # sentinel Shard object returned by get_shard_for
        self.mapping_value = 42
        self.user = mock.Mock(is_staff=True, pk=7)
        self.recorder = _ShardActivationRecorder()
        use_shard_patcher = mock.patch('djanquiltdb.contrib.quilt_admin.utils.use_shard', self.recorder)
        use_shard_patcher.start()
        self.addCleanup(use_shard_patcher.stop)
        self.get_shard_for = mock.patch(
            'djanquiltdb.contrib.quilt_admin.utils.get_shard_for', return_value=self.shard
        ).start()
        self.addCleanup(mock.patch.stopall)

    def test_reads_attribute_value_from_wrapped_user(self):
        proxy = CrossShardMappingUserProxy(self.user, self.mapping_value)

        self.assertIs(proxy.is_staff, True)
        self.assertEqual(proxy.pk, 7)

    def test_get_shard_for_called_once_across_many_accesses(self):
        proxy = CrossShardMappingUserProxy(self.user, self.mapping_value)

        for _ in range(50):
            proxy.is_staff
            proxy.pk

        self.get_shard_for.assert_called_once_with(self.mapping_value)

    def test_repeated_access_of_same_attribute_activates_shard_once(self):
        proxy = CrossShardMappingUserProxy(self.user, self.mapping_value)

        for _ in range(200):
            proxy.is_staff

        self.assertEqual(self.recorder.call_count, 1)

    def test_activation_uses_resolved_shard_and_preserves_mapping_lock_key(self):
        proxy = CrossShardMappingUserProxy(self.user, self.mapping_value)

        proxy.is_staff

        (args, kwargs) = self.recorder.calls[0]
        self.assertIs(args[0], self.shard)
        self.assertEqual(kwargs.get('mapping_value'), self.mapping_value)

    def test_shard_context_is_active_during_attribute_resolution(self):
        user = _RecordingUser(self.recorder)
        proxy = CrossShardMappingUserProxy(user, self.mapping_value)

        self.assertEqual(proxy.name, 'Jon Snow')
        self.assertEqual(user.active_during_name_access, 1)
        self.assertEqual(self.recorder.active, 0)

    def test_internal_user_attribute_is_not_proxied(self):
        proxy = CrossShardMappingUserProxy(self.user, self.mapping_value)

        self.assertIs(proxy._user, self.user)
        self.assertEqual(self.recorder.call_count, 0)
        self.get_shard_for.assert_not_called()


class CrossShardProxyIdentityTests(SimpleTestCase):
    """The middleware branches on the concrete proxy classes, so their identities must stay distinct."""

    def test_proxy_classes_are_distinct(self):
        user = mock.Mock()

        id_proxy = CrossShardUserProxy(user, object())
        mapping_proxy = CrossShardMappingUserProxy(user, 1)

        self.assertIsInstance(id_proxy, CrossShardUserProxy)
        self.assertNotIsInstance(id_proxy, CrossShardMappingUserProxy)
        self.assertIsInstance(mapping_proxy, CrossShardMappingUserProxy)
        self.assertNotIsInstance(mapping_proxy, CrossShardUserProxy)
