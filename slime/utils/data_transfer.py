import os
from functools import cache
from typing import Any


def put_transfer_data(args: Any, data: dict[str, Any], partition: str = "default") -> Any:
    if getattr(args, "transfer_backend", "ray") == "ray":
        import ray
        from slime.utils.misc import Box

        return Box(ray.put(data))

    transfer = _mooncake_transfer(args)
    ref = transfer.put_legacy_dict(
        data,
        namespace="slime",
        partition=partition,
        stage="rollout",
        chunk_bytes=(getattr(args, "mooncake_store_init_kwargs", None) or {}).get("chunk_bytes"),
    )
    from mooncake.structured_object_store import export_dataproto_ref

    return export_dataproto_ref(ref)


def get_transfer_data(args: Any, ref: Any) -> dict[str, Any]:
    if getattr(args, "transfer_backend", "ray") == "ray":
        import ray
        from slime.utils.misc import Box

        return ray.get(ref.inner if isinstance(ref, Box) else ref)
    return _mooncake_transfer(args).get_legacy_dict(ref)


def cleanup_transfer_refs(args: Any, refs: list[Any] | None) -> None:
    if getattr(args, "transfer_backend", "ray") == "ray" or refs is None:
        return
    transfer = _mooncake_transfer(args)
    for ref in refs:
        transfer.remove_legacy_dict(ref)


@cache
def _mooncake_transfer_cached(config_items: tuple[tuple[str, Any], ...]):
    from mooncake.store import MooncakeDistributedStore
    from mooncake.structured_object_store import MooncakeBundleTransfer

    config = dict(config_items)
    store = MooncakeDistributedStore()
    ret = store.setup(
        config.get("local_hostname") or _local_hostname(),
        config.get("metadata_server") or os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE"),
        int(config.get("global_segment_size") or os.getenv("MOONCAKE_GLOBAL_SEGMENT_SIZE", 4 * 1024**3)),
        int(config.get("local_buffer_size") or os.getenv("MOONCAKE_LOCAL_BUFFER_SIZE", 2 * 1024**3)),
        config.get("protocol") or os.getenv("MOONCAKE_PROTOCOL", "tcp"),
        config.get("device_name") or config.get("rdma_devices") or os.getenv("MOONCAKE_DEVICE", ""),
        config.get("master_server_address") or config.get("master_server_addr") or os.getenv("MOONCAKE_MASTER"),
    )
    if ret:
        raise RuntimeError(f"Mooncake store setup failed: {ret}")
    return MooncakeBundleTransfer(store, key_prefix="slime-rollout")


def _mooncake_transfer(args: Any):
    config = getattr(args, "mooncake_store_init_kwargs", None) or {}
    return _mooncake_transfer_cached(tuple(sorted(config.items())))


def _local_hostname() -> str:
    value = os.getenv("MOONCAKE_LOCAL_HOSTNAME")
    if value and value not in ("localhost", "127.0.0.1"):
        return value
    try:
        import ray

        if ray.is_initialized():
            return ray.util.get_node_ip_address()
    except Exception:
        pass
    return os.getenv("LOCAL_HOSTNAME", os.getenv("HOSTNAME", "127.0.0.1"))
