"""分区文件清单与入湖回放恢复的场景化测试。

这些测试覆盖需求中的 A/B/C/D 故事线，同时固定核心返回字段，避免后续重构
破坏“断点续取、版本核对、回执恢复和差额核对”。
"""
from __future__ import annotations

import pytest

from app.lake_store import LakeStore
from app.services.partition_replay import PartitionReplayService


@pytest.fixture()
def story_store() -> LakeStore:
    store = LakeStore()
    table = "replay_story"
    store.register_file(table, "p1", 3, 100, "hash-a", file_id="A", created_at=8)
    store.register_file(table, "p1", 4, 100, "hash-b", file_id="B", created_at=8)
    store.register_file(table, "p2", 2, 100, "hash-c", file_id="C", created_at=8)
    store.create_snapshot(
        table, ["A", "B", "C"], snapshot_id="S1", created_at=8, status="available"
    )
    store.register_file(
        table, "p1", 5, 120, "hash-d", file_id="D", created_at=9, replaces=["B"]
    )
    store.create_snapshot(
        table, ["A", "D", "C"], parent_snapshot="S1",
        snapshot_id="S2", created_at=9, status="generating",
    )
    # 另一个生成中的空快照不能被解释为空分区清单。
    store.create_snapshot(
        table, [], parent_snapshot="S2", snapshot_id="S3-generating",
        created_at=10, status="generating",
    )
    return store


class TestPartitionManifest:
    def test_timepoint_uses_available_s1_and_partition_isolation(self, story_store):
        manifest = story_store.build_partition_manifest(
            "replay_story", 10, partition_key="p1"
        )
        assert manifest.success
        assert manifest.snapshot_id == "S1"
        assert [f.file_id for f in manifest.files] == ["A", "B"]
        assert [f.file_rows for f in manifest.files] == [3, 4]
        assert manifest.total_rows == 7
        assert manifest.partition_keys == ["p1"]
        assert "C" not in {f.file_id for f in manifest.files}
        assert manifest.generating_snapshot_ids

        all_manifest = story_store.build_partition_manifest("replay_story", 10)
        assert all_manifest.partition_keys == ["p1", "p2"]
        assert [f.file_id for f in all_manifest.partitions["p2"]] == ["C"]

    def test_fresh_query_picks_newly_available_s2(self, story_store):
        story_store.mark_snapshot_available("replay_story", "S2")
        manifest = story_store.build_partition_manifest("replay_story", 10)
        assert manifest.snapshot_id == "S2"
        assert [f.file_id for f in manifest.partitions["p1"]] == ["A", "D"]
        assert [f.file_rows for f in manifest.partitions["p1"]] == [3, 5]
        # S3 仍 generating，不能产生额外的“空分区”。
        assert manifest.partition_keys == ["p1", "p2"]

        result = PartitionReplayService(story_store).read_at_timepoint(
            "replay_story", 10
        )
        assert result["success"]
        assert result["snapshot_id"] == "S2"


class TestReplayStory:
    def test_pinned_s1_continues_b_and_reports_d_alternative(self, story_store):
        story_store.start_replay(
            "replay_story", 10, "r-pinned", partition_key="p1", page_size=1
        )
        page = story_store.fetch_page("r-pinned")
        assert page.file_ids == ["A"]
        ack = story_store.ack_files("r-pinned", ["A"])
        assert ack.processed_rows == 3
        story_store.pause_replay("r-pinned")

        story_store.mark_snapshot_available("replay_story", "S2")
        recovery = story_store.recover_replay("r-pinned")
        assert recovery.adopted_snapshot_id == "S1"
        assert recovery.latest_snapshot_id == "S2"
        assert recovery.next_file_id == "B"
        b_decision = next(d for d in recovery.decisions if d.file_id == "B")
        assert b_decision.disposition == "continue"
        assert b_decision.deliver is True
        assert b_decision.available_successor_file_id == "D"

        page_b = story_store.fetch_page("r-pinned")
        assert page_b.file_ids == ["B"]
        assert page_b.snapshot_id == "S1"
        story_store.ack_files("r-pinned", ["B"])
        detail = story_store.get_replay_detail("r-pinned")
        assert detail.processed_rows == 7
        assert detail.row_recomputation.by_partition == {"p1": 7}

        report = story_store.close_replay("r-pinned", force=True)
        alternative = report.unconsumed_alternatives[0]
        assert alternative.file_id == "D"
        assert alternative.file_rows == 5
        assert report.row_differences[0]["row_difference"] == 1

    def test_adopt_s2_delivers_d_instead_of_b_and_skips_consumed_a(self, story_store):
        service = PartitionReplayService(story_store)
        service.start_replay(
            lake_table="replay_story", as_of=10, replay_id="r-s2",
            partition_key="p1", page_size=1,
        )
        assert service.fetch_page("r-s2").file_ids == ["A"]
        service.ack_files("r-s2", ["A"])
        service.pause_replay("r-s2")
        story_store.mark_snapshot_available("replay_story", "S2")

        recovery = service.recover_replay("r-s2", snapshot_id="S2")
        assert recovery.adopted_snapshot_id == "S2"
        assert recovery.next_file_id == "D"
        b_decision = next(d for d in recovery.decisions if d.file_id == "B")
        assert b_decision.disposition == "superseded"
        assert b_decision.successor_file_id == "D"

        assert service.fetch_page("r-s2").file_ids == ["D"]
        service.ack_files("r-s2", ["D"])
        detail = service.get_replay_detail("r-s2")
        assert detail.consumed_file_ids == ["A", "D"]
        assert detail.processed_rows == 8
        assert detail.next_start.next_offset == 2

        report = service.close_replay("r-s2")
        assert report.success
        alt = report.unconsumed_alternatives[0]
        assert alt.file_id == "B"
        assert alt.file_rows == 4
        assert report.row_differences[0]["difference_rows"] == 1

        # A 已消费，再次取页不能重复交付。
        assert service.fetch_page("r-s2").file_ids == []

    def test_lost_commit_response_is_resolved_by_receipt(self, story_store):
        story_store.start_replay(
            "replay_story", 10, "r-lost", partition_key="p1", page_size=1
        )
        story_store.fetch_page("r-lost")
        session = story_store.get_replay("r-lost")
        assert session.unacked_file_ids == ["A"]

        # 入湖端实际提交并留下回执，但提交响应丢失，回放端没有完成 ack。
        receipt = story_store.record_receipt(
            receipt_id="receipt-A",
            lake_table="replay_story",
            replay_id="r-lost",
            file_ids=["A"],
            snapshot_id="S1",
            status="ingested",
            response_lost=True,
        )
        assert receipt.committed is True

        recovery = story_store.recover_replay("r-lost")
        assert "A" in recovery.consumed_file_ids
        assert recovery.unacked_file_ids == []
        assert recovery.next_file_id == "B"
        detail = story_store.get_replay_detail("r-lost")
        a_item = next(f for f in detail.files if f.file_id == "A")
        assert a_item.state == "consumed"
        assert a_item.recovered_from_receipt is True
        assert a_item.version_verified is True
        assert detail.processed_rows == 3

        # 无回执的未确认文件会重新交付；这里 A 有回执，因此下一页是 B。
        page = story_store.fetch_page("r-lost")
        assert page.file_ids == ["B"]

    def test_restart_from_head_selects_s2(self, story_store):
        story_store.start_replay("replay_story", 10, "r-old", partition_key="p1")
        assert story_store.get_replay("r-old").snapshot_id == "S1"
        story_store.mark_snapshot_available("replay_story", "S2")

        restarted = story_store.restart_replay("r-old")
        assert restarted.snapshot_id == "S2"
        assert restarted.attempt == 2
        assert restarted.next_file_id == "A"
        assert [item["file_id"] for item in restarted["items"]] == ["A", "D"]
