from netobserv.connectors.base import BaseConnector, CollectionContext, DiscoveryTarget, ConnectorAuthError, RawCollectionResult


class SuccessConnector(BaseConnector):
    connector_type = "test"

    async def _collect_impl(self, target, context):
        return RawCollectionResult(target=target, workflow_id=context.workflow_id, snapshot_id=context.snapshot_id, connector_type=self.connector_type, success=True)


class AuthFailConnector(BaseConnector):
    connector_type = "test"

    async def _collect_impl(self, target, context):
        raise ConnectorAuthError("bad auth")


async def test_base_connector_collect_success():
    connector = SuccessConnector()
    target = DiscoveryTarget(target_id="1", hostname="r1")
    context = CollectionContext(workflow_id="wf", snapshot_id="snap")
    result = await connector.collect(target, context)
    assert result.success is True
    assert result.elapsed_seconds is not None


async def test_base_connector_auth_failure_returns_failed_result():
    connector = AuthFailConnector()
    target = DiscoveryTarget(target_id="1", hostname="r1")
    context = CollectionContext(workflow_id="wf", snapshot_id="snap")
    result = await connector.collect(target, context)
    assert result.success is False
    assert result.error_class == "ConnectorAuthError"
