"""Task service tests: fencing, leases, checkpoints, retirement, cursors.

These exercise the service and store layers directly rather than through HTTP,
so a failure points at the rule that broke instead of at routing. The fencing
invariants are the point of the module, so most of the file is here.
"""

from __future__ import annotations

import asyncio

import pytest

from a2x_registry.tasks.errors import TaskError


def run(coro):
    return asyncio.run(coro)


def _create(started, actor="p_alice", title="t", **kwargs):
    return run(started.create_task(actor=actor, owner_dataset="default", title=title, **kwargs))


def _acquire(started, task_id, actor="p_alice", device="k_phone", **kwargs):
    return run(started.acquire(actor=actor, device=device, task_id=task_id, **kwargs))


def _append(started, task_id, fence, actor="p_alice", device="k_phone", payload=None, **kwargs):
    return run(
        started.append_message(
            actor=actor,
            device=device,
            task_id=task_id,
            fence=fence,
            payload={"text": payload} if payload else {"text": "hi"},
            **kwargs,
        )
    )


# ── Creation ─────────────────────────────────────────────────────────────


def test_create_writes_bootstrap_system_message(started):
    task = _create(started, title="triage")
    assert task.id == 1
    assert task.owner_id == "p_alice"
    assert task.state == "pending"
    # seq_counter starts at 1 because the creation event occupies seq=1.
    assert task.seq_counter == 1

    page = run(started.store.fetch_page(task.id, after_seq=0, limit=10))
    assert [m.seq for m in page.messages] == [1]
    assert page.messages[0].kind == "system"
    assert page.messages[0].payload["event"] == "task_created"


def test_create_rejects_control_characters_in_title(started):
    with pytest.raises(TaskError) as excinfo:
        _create(started, title="line\nbreak")
    assert excinfo.value.code == "task_title_invalid"


def test_create_rejects_overlong_title(started):
    with pytest.raises(TaskError) as excinfo:
        _create(started, title="x" * 500)
    assert excinfo.value.code == "task_title_invalid"


def test_task_quota_is_enforced(started, config):
    for i in range(config.max_tasks_per_principal):
        _create(started, title=f"t{i}")
    with pytest.raises(TaskError) as excinfo:
        _create(started, title="one-too-many")
    assert excinfo.value.code == "task_quota_exceeded"


# ── Ownership disclosure ─────────────────────────────────────────────────


def test_non_owner_cannot_read_task(started):
    task = _create(started)
    with pytest.raises(TaskError) as excinfo:
        run(started.get_task(actor="p_mallory", task_id=task.id))
    assert excinfo.value.status_code == 404


def test_non_owner_cannot_acquire(started):
    task = _create(started)
    with pytest.raises(TaskError) as excinfo:
        _acquire(started, task.id, actor="p_mallory", device="k_mallory")
    assert excinfo.value.status_code == 404


# ── Acquire / fencing ────────────────────────────────────────────────────


def test_acquire_bumps_fence_and_sets_holder(started):
    task = _create(started)
    acquired = _acquire(started, task.id)
    assert acquired["fence"] == 1
    assert acquired["holder"] == "k_phone"
    assert acquired["state"] == "working"


def test_second_acquire_takes_over_and_bumps_fence_again(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")  # fence 1
    taken = _acquire(started, task.id, device="k_pc")  # fence 2
    assert taken["fence"] == 2
    assert taken["holder"] == "k_pc"


def test_felled_device_write_is_rejected_with_409(started):
    """The core invariant: after a takeover, the old fence must not write."""
    task = _create(started)
    _acquire(started, task.id, device="k_phone")  # phone holds fence 1
    _acquire(started, task.id, device="k_pc")  # pc takes over, fence 2

    with pytest.raises(TaskError) as excinfo:
        _append(started, task.id, fence=1, device="k_phone")
    assert excinfo.value.code == "task_fence_stale"
    assert excinfo.value.status_code == 409
    # No row was written by the felled device.
    assert run(started.store.latest_seq(task.id)) == 1


def test_acquire_terminal_task_is_rejected(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    run(started.release(actor="p_alice", device="k_phone", task_id=task.id, fence=1, next_state="completed"))
    with pytest.raises(TaskError) as excinfo:
        _acquire(started, task.id, device="k_pc")
    assert excinfo.value.code == "task_terminal"


def test_append_requires_a_device_key(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    with pytest.raises(TaskError) as excinfo:
        run(
            started.append_message(
                actor="p_alice", device=None, task_id=task.id, fence=1, payload={"text": "x"}
            )
        )
    assert excinfo.value.code == "task_device_required"


# ── Sequence allocation ──────────────────────────────────────────────────


def test_append_allocates_monotonic_seq(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    seqs = [
        _append(started, task.id, fence=1, payload=f"m{i}").seq for i in range(3)
    ]
    assert seqs == [2, 3, 4]


def test_append_is_idempotent(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    first = _append(started, task.id, fence=1, payload="retry", client_msg_id="c_1")
    second = _append(started, task.id, fence=1, payload="retry", client_msg_id="c_1")
    assert first.seq == second.seq
    assert run(started.store.latest_seq(task.id)) == first.seq


def test_client_cannot_write_reserved_kinds(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    for kind in ("event", "system"):
        with pytest.raises(TaskError) as excinfo:
            run(
                started.append_message(
                    actor="p_alice", device="k_phone", task_id=task.id,
                    fence=1, payload={"x": 1}, kind=kind,
                )
            )
        assert excinfo.value.code == "task_kind_reserved"


def test_terminal_task_cannot_append(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    run(started.release(actor="p_alice", device="k_phone", task_id=task.id, fence=1, next_state="failed"))
    with pytest.raises(TaskError) as excinfo:
        _append(started, task.id, fence=1)
    assert excinfo.value.code == "task_terminal"


# ── Renew / release / checkpoint ─────────────────────────────────────────


def test_renew_extends_lease_for_the_current_holder(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone", ttl_seconds=10)
    result = run(
        started.renew(actor="p_alice", device="k_phone", task_id=task.id, fence=1, ttl_seconds=20)
    )
    assert result["fence"] == 1
    refreshed = run(started.store.get_task(task.id))
    assert refreshed.lease_expires_at is not None


def test_renew_with_stale_fence_rejects(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")  # fence 1
    _acquire(started, task.id, device="k_pc")  # fence 2
    with pytest.raises(TaskError) as excinfo:
        run(started.renew(actor="p_alice", device="k_phone", task_id=task.id, fence=1))
    assert excinfo.value.code == "task_fence_stale"


def test_release_and_checkpoint_are_atomic(started):
    task = _create(started)
    acquired = _acquire(started, task.id, device="k_phone")
    fence = acquired["fence"]
    _append(started, task.id, fence=fence)
    _append(started, task.id, fence=fence)

    run(
        started.release(
            actor="p_alice", device="k_phone", task_id=task.id, fence=fence,
            next_state="input_required", checkpoint={"done": False}, checkpoint_seq=3,
        )
    )
    released = run(started.store.get_task(task.id))
    assert released.lease_holder is None
    assert released.state == "input_required"
    assert released.checkpoint == {"done": False}
    assert released.checkpoint_seq == 3


def test_release_preserves_checkpoint_when_omitted(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    run(started.set_checkpoint(actor="p_alice", device="k_phone", task_id=task.id, fence=1, checkpoint={"a": 1}, checkpoint_seq=2))
    run(started.release(actor="p_alice", device="k_phone", task_id=task.id, fence=1, next_state="input_required"))
    released = run(started.store.get_task(task.id))
    assert released.checkpoint == {"a": 1}


def test_release_rejects_working_as_a_target_state(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    with pytest.raises(TaskError) as excinfo:
        run(started.release(actor="p_alice", device="k_phone", task_id=task.id, fence=1, next_state="working"))
    assert excinfo.value.code == "task_state_invalid"


def test_release_with_stale_fence_rejects(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")  # fence 1
    _acquire(started, task.id, device="k_pc")  # fence 2
    with pytest.raises(TaskError) as excinfo:
        run(started.release(actor="p_alice", device="k_phone", task_id=task.id, fence=1, next_state="input_required"))
    assert excinfo.value.code == "task_fence_stale"


def test_checkpoint_with_stale_fence_rejects(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")  # fence 1
    _acquire(started, task.id, device="k_pc")  # fence 2
    with pytest.raises(TaskError) as excinfo:
        run(started.set_checkpoint(actor="p_alice", device="k_phone", task_id=task.id, fence=1, checkpoint={"x": 1}, checkpoint_seq=5))
    assert excinfo.value.code == "task_fence_stale"


# ── Graceful handoff degrades to force ───────────────────────────────────


def test_graceful_acquire_still_takes_the_lease(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    # graceful mode waits (bounded) then forces; it must still succeed.
    taken = run(started.acquire(actor="p_alice", device="k_pc", task_id=task.id, mode="graceful"))
    assert taken["holder"] == "k_pc"


# ── Sweeper (crash recovery) ─────────────────────────────────────────────


def test_reap_releases_expired_lease_to_input_required(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone", ttl_seconds=10)
    # Pin the clock past the TTL: everything is dead, no one renews.
    future = run(started.store.get_task(task.id)).lease_expires_at + 1
    assert run(started.store.reap_expired(future)) == 1

    reaped = run(started.store.get_task(task.id))
    assert reaped.lease_holder is None
    assert reaped.state == "input_required"


# ── Cursor / sync ────────────────────────────────────────────────────────


def test_cursor_never_rewinds(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    _append(started, task.id, fence=1)

    forward = run(started.ack(actor="p_alice", device="k_phone", task_id=task.id, seq=2))
    assert forward["last_read_seq"] == 2
    stale = run(started.ack(actor="p_alice", device="k_phone", task_id=task.id, seq=1))
    assert stale["last_read_seq"] == 2


def test_ack_beyond_latest_is_rejected(started):
    task = _create(started)
    with pytest.raises(TaskError) as excinfo:
        run(started.ack(actor="p_alice", device="k_phone", task_id=task.id, seq=999))
    assert excinfo.value.code == "task_seq_ahead"


def test_new_device_resumes_from_checkpoint_not_seq_one(started):
    """Pitfall 6: a takeover device must not replay the whole history."""
    task = _create(started)
    _acquire(started, task.id, device="k_phone")  # fence 1
    _append(started, task.id, fence=1)
    _append(started, task.id, fence=1)
    _append(started, task.id, fence=1)  # latest seq = 4
    run(
        started.release(
            actor="p_alice", device="k_phone", task_id=task.id, fence=1,
            next_state="input_required", checkpoint={"up to": 4}, checkpoint_seq=4,
        )
    )
    # PC has never read this task: it should start at checkpoint_seq (4), not 0.
    page = run(
        started.fetch_messages(actor="p_alice", device="k_pc", task_id=task.id)
    )
    assert page["checkpoint_seq"] == 4
    assert page["messages"] == []


# ── Archive ──────────────────────────────────────────────────────────────


def test_archive_hides_from_listing_but_keeps_readable(started):
    task = _create(started)
    run(started.update_task(actor="p_alice", task_id=task.id, archived=True))
    assert run(started.list_tasks(actor="p_alice", limit=10)) == []
    detail = run(started.get_task(actor="p_alice", task_id=task.id))
    assert detail["task"]["archived"] is True


def test_archive_blocks_writes(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    run(started.update_task(actor="p_alice", task_id=task.id, archived=True))
    with pytest.raises(TaskError) as excinfo:
        _append(started, task.id, fence=1)
    assert excinfo.value.code == "task_archived"


# ── Retirement ───────────────────────────────────────────────────────────


def test_retire_requires_terminal_state(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    with pytest.raises(TaskError) as excinfo:
        run(started.retire(actor="p_alice", task_id=task.id))
    assert excinfo.value.code == "task_not_retirable"


def test_retire_removes_intermediate_state_but_keeps_the_record(started):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    _append(started, task.id, fence=1)
    run(started.release(actor="p_alice", device="k_phone", task_id=task.id, fence=1, next_state="completed"))

    result = run(started.retire(actor="p_alice", task_id=task.id))
    assert result["status"] == "retired"

    # The task row survives; the messages and checkpoint do not.
    assert run(started.store.get_task(task.id)) is not None
    assert run(started.store.fetch_page(task.id, after_seq=0, limit=10)).messages == []
    assert run(started.store.get_task(task.id)).checkpoint is None


# ── Payload / rate limiting ──────────────────────────────────────────────


def test_payload_size_is_bounded(started, config):
    task = _create(started)
    _acquire(started, task.id, device="k_phone")
    with pytest.raises(TaskError) as excinfo:
        run(
            started.append_message(
                actor="p_alice", device="k_phone", task_id=task.id, fence=1,
                payload={"text": "x" * (config.max_message_bytes + 1)},
            )
        )
    assert excinfo.value.code == "task_message_too_large"


def test_rate_limit_triggers(service):
    async def scenario():
        await service.start()
        tight = service.config.__class__(**{**service.config.__dict__, "rate_limit_per_task": 2})
        service.config = tight
        task = await service.create_task(actor="p_alice", owner_dataset="default", title="busy")
        await service.acquire(actor="p_alice", device="k_phone", task_id=task.id)
        await service.append_message(actor="p_alice", device="k_phone", task_id=task.id, fence=1, payload={"text": "1"})
        await service.append_message(actor="p_alice", device="k_phone", task_id=task.id, fence=1, payload={"text": "2"})
        try:
            await service.append_message(actor="p_alice", device="k_phone", task_id=task.id, fence=1, payload={"text": "3"})
        except TaskError as exc:
            assert exc.code == "task_rate_limited"
        else:  # pragma: no cover
            raise AssertionError("rate limit did not trigger")
        finally:
            await service.stop()

    run(scenario())