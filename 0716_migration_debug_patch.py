from pathlib import Path


def patch_once(path: str, old: str, new: str, marker: str) -> None:
    p = Path(path)
    s = p.read_text()
    if marker in s:
        print(f"[skip] {path}: {marker}")
        return
    if old not in s:
        raise SystemExit(f"[fail] target not found in {path}: {marker}")
    p.write_text(s.replace(old, new, 1))
    print(f"[patch] {path}: {marker}")


def patch_manager() -> None:
    old = """            instance_infos = self.global_scheduler.instance_info
            migration_tasks = self.global_scheduler.push_migrations(instance_infos)
"""
    new = """            instance_infos = self.global_scheduler.instance_info
            logger.warning(
                "MIGRATION_DEBUG_MANAGER enter_migrate instance_count=%s instances=%s",
                len(instance_infos),
                [
                    (
                        iid,
                        str(getattr(info, "migration_load_metric", None)),
                        str(getattr(info, "migration_load_metric_after_migrate_out", None)),
                        str(getattr(info, "migration_load_metric_after_migrate_in", None)),
                    )
                    for iid, info in instance_infos.items()
                ],
            )
            migration_tasks = self.global_scheduler.push_migrations(instance_infos)
            logger.warning("MIGRATION_DEBUG_MANAGER migration_tasks=%s", migration_tasks)
"""
    patch_once("llumnix/manager.py", old, new, "MIGRATION_DEBUG_MANAGER enter_migrate")

    old = """                    asyncio.create_task(asyncio_wait_for_ray_remote_call_with_timeout(
                        self.instances[src_instance_id].migrate_out,
                        dst_instance_actor,
                        self.instance_context[dst_instance_id],
                        migration_type
                    ))
"""
    new = """                    logger.warning(
                        "MIGRATION_DEBUG_MANAGER call_migrate_out type=%s src=%s dst=%s",
                        migration_type,
                        src_instance_id,
                        dst_instance_id,
                    )
                    asyncio.create_task(asyncio_wait_for_ray_remote_call_with_timeout(
                        self.instances[src_instance_id].migrate_out,
                        dst_instance_actor,
                        self.instance_context[dst_instance_id],
                        migration_type
                    ))
"""
    patch_once("llumnix/manager.py", old, new, "MIGRATION_DEBUG_MANAGER call_migrate_out")


def patch_global_scheduler() -> None:
    old = """        pair_instance_ids = migration_policy.pair_migration(src_instance_infos, dst_instance_infos)
        pair_instance_ids = self._pair_filter(pair_instance_ids)
        return pair_instance_ids
"""
    new = """        pair_instance_ids = migration_policy.pair_migration(src_instance_infos, dst_instance_infos)
        logger.warning(
            "MIGRATION_DEBUG_PAIR policy=%s src=%s dst=%s raw_pairs=%s",
            type(migration_policy).__name__,
            [(i.instance_id, str(i.migration_load_metric)) for i in src_instance_infos],
            [(i.instance_id, str(i.migration_load_metric)) for i in dst_instance_infos],
            pair_instance_ids,
        )
        pair_instance_ids = self._pair_filter(pair_instance_ids)
        if not pair_instance_ids and len(instance_info) >= 2:
            all_infos = sorted(
                instance_info.values(),
                key=lambda i: i.migration_load_metric,
                reverse=True,
            )
            src = all_infos[0]
            dst = all_infos[-1]
            if src.instance_id != dst.instance_id:
                pair_instance_ids = [(src.instance_id, dst.instance_id)]
                logger.warning(
                    "MIGRATION_DEBUG_PAIR force_pair_all src=%s dst=%s all=%s",
                    src.instance_id,
                    dst.instance_id,
                    [(i.instance_id, str(i.migration_load_metric)) for i in all_infos],
                )
        logger.warning("MIGRATION_DEBUG_PAIR final_pairs=%s", pair_instance_ids)
        return pair_instance_ids
"""
    patch_once(
        "llumnix/global_scheduler/migration_scheduler.py",
        old,
        new,
        "MIGRATION_DEBUG_PAIR force_pair_all",
    )


def patch_local_scheduler() -> None:
    p = Path("llumnix/llumlet/local_migration_scheduler.py")
    s = p.read_text()
    if "MIGRATION_DEBUG_LOCAL selected_requests" in s:
        print("[skip] llumnix/llumlet/local_migration_scheduler.py: MIGRATION_DEBUG_LOCAL")
        return

    if "logger = init_logger(__name__)" not in s:
        import_anchor = "from llumnix.utils import MigrationType\n"
        if import_anchor not in s:
            raise SystemExit("[fail] local scheduler import anchor not found")
        s = s.replace(
            import_anchor,
            import_anchor
            + "from llumnix.logging.logger import init_logger\n\n"
            + "logger = init_logger(__name__)\n",
            1,
        )

    old = """        return migrate_out_requests
"""
    new = """        try:
            running_queue = list(self.backend_engine.get_running_queue())
            waiting_queue = list(self.backend_engine.get_waiting_queue())
            logger.warning(
                "MIGRATION_DEBUG_LOCAL selected_requests policy=%s migration_type=%s running=%s waiting=%s selected=%s",
                self.request_migration_policy,
                migration_type,
                [
                    (
                        getattr(req, "request_id", None),
                        str(getattr(req, "llumnix_status", None)),
                        str(getattr(req, "inference_type", None)),
                        getattr(req, "request_len", None),
                        getattr(req, "output_len", None),
                        getattr(req, "expected_steps", None),
                        getattr(req, "is_migrating", None),
                    )
                    for req in running_queue
                ],
                [
                    (
                        getattr(req, "request_id", None),
                        str(getattr(req, "llumnix_status", None)),
                        getattr(req, "request_len", None),
                        getattr(req, "try_schedule_times", None),
                    )
                    for req in waiting_queue
                ],
                [getattr(req, "request_id", None) for req in migrate_out_requests],
            )
        except Exception:
            logger.exception("MIGRATION_DEBUG_LOCAL failed to log request queues")
        return migrate_out_requests
"""
    if old not in s:
        raise SystemExit("[fail] local scheduler return target not found")
    p.write_text(s.replace(old, new, 1))
    print("[patch] llumnix/llumlet/local_migration_scheduler.py: MIGRATION_DEBUG_LOCAL")


if __name__ == "__main__":
    patch_manager()
    patch_global_scheduler()
    patch_local_scheduler()
    print("[done] migration debug patch installed")
