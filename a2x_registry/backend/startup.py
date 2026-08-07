"""Registry control-plane startup and shutdown."""

from __future__ import annotations

import logging
import os
from typing import Any

from a2x_registry.common.paths import database_dir


logger = logging.getLogger("uvicorn")
runtime_state: dict[str, Any] = {}


def startup_registry() -> None:
    """Load registry state and start optional auth, heartbeat, and cluster modules."""
    from a2x_registry.backend.routers.dataset import init_registry_service

    registry_svc = init_registry_service(database_dir())
    registry_svc.startup()

    try:
        from a2x_registry.auth.deps import set_auth_store
        from a2x_registry.auth.store import AuthStore

        auth_store = AuthStore.load_or_none()
        set_auth_store(auth_store)
        if auth_store is None:
            logger.info("Auth not initialized; registry runs in anonymous mode")
        else:
            logger.info("Auth store loaded (%d principals)", len(auth_store.list_principals()))
    except Exception as exc:  # noqa: BLE001 - optional module must not block startup
        logger.error("Auth store load failed: %s", exc, exc_info=True)
        from a2x_registry.auth.deps import set_auth_store

        set_auth_store(None)

    try:
        from a2x_registry.heartbeat.deps import set_heartbeat_store
        from a2x_registry.heartbeat.store import HeartbeatStore
        from a2x_registry.heartbeat.sweeper import HeartbeatSweeper

        heartbeat_store = HeartbeatStore(config_provider=registry_svc.get_lease_config)
        registry_svc.set_unhealthy_check(heartbeat_store.is_unhealthy)
        recovered = [
            (dataset, entry.service_id, entry.lease_ttl)
            for dataset in registry_svc.list_datasets()
            for entry in registry_svc.list_entries(dataset)
            if entry.lease_ttl is not None
        ]
        if recovered:
            heartbeat_store.recover_from_persisted(recovered)
        set_heartbeat_store(heartbeat_store)
        sweeper = HeartbeatSweeper(registry_svc, heartbeat_store, period=5.0)
        sweeper.start()
        runtime_state["heartbeat_sweeper"] = sweeper
        logger.info("Heartbeat store loaded (%d leases recovered)", len(recovered))
    except Exception as exc:  # noqa: BLE001 - optional module must not block startup
        logger.error("Heartbeat init failed: %s", exc, exc_info=True)
        from a2x_registry.heartbeat.deps import set_heartbeat_store

        set_heartbeat_store(None)

    try:
        from a2x_registry.auth.deps import get_auth_store
        from a2x_registry.cluster.config import ClusterConfig
        from a2x_registry.cluster.deps import set_cluster_store
        from a2x_registry.cluster.store import ClusterStore

        cluster_store = ClusterStore.load_or_none(
            config=ClusterConfig.from_env(),
            registry_svc=registry_svc,
            advertise=os.environ.get("A2X_REGISTRY_CLUSTER_ADVERTISE", ""),
            auth_store_getter=get_auth_store,
        )
        set_cluster_store(cluster_store)
        if cluster_store is None:
            logger.info("Cluster module not initialized (standalone)")
            return

        from a2x_registry.cluster.membership import MembershipStore
        from a2x_registry.cluster.sweepers import AntiEntropySweeper, KeepaliveMonitor

        cluster_store.membership = MembershipStore(cluster_store)
        registry_svc.set_on_mutation(cluster_store.on_local_mutation)
        anti_entropy = AntiEntropySweeper(
            cluster_store,
            period=cluster_store.config.anti_entropy_interval,
        )
        keepalive = KeepaliveMonitor(
            cluster_store,
            period=cluster_store.config.keepalive_interval,
        )
        anti_entropy.start()
        keepalive.start()
        runtime_state["cluster_anti_entropy"] = anti_entropy
        runtime_state["cluster_keepalive"] = keepalive
        logger.info("Cluster module loaded (node_id=%s)", cluster_store.node_id)
    except Exception as exc:  # noqa: BLE001 - optional module must not block startup
        logger.error("Cluster init failed: %s", exc, exc_info=True)
        from a2x_registry.cluster.deps import set_cluster_store

        set_cluster_store(None)


def shutdown_registry() -> None:
    """Stop registry background daemons and close the optional cluster store."""
    for key in ("cluster_anti_entropy", "cluster_keepalive", "heartbeat_sweeper"):
        daemon = runtime_state.pop(key, None)
        if daemon is not None:
            try:
                daemon.stop()
            except Exception:  # noqa: BLE001 - shutdown must not raise
                pass

    try:
        from a2x_registry.cluster.deps import get_cluster_store, set_cluster_store

        store = get_cluster_store()
        if store is not None:
            store.close()
        set_cluster_store(None)
    except Exception:  # noqa: BLE001 - shutdown must not raise
        pass

    try:
        from a2x_registry.heartbeat.deps import set_heartbeat_store

        set_heartbeat_store(None)
    except Exception:  # noqa: BLE001 - shutdown must not raise
        pass
