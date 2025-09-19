from __future__ import annotations
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
import uuid

from langgraph.graph import StateGraph, END
from langgraph.store.memory import InMemoryStore
from langgraph.types import Interrupt
from IPython.display import Image, display

# ==========
# Utilities
# ==========
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ==========
# Audit metadata model
# ==========
@dataclass
class InstanceMeta:
    instance_id: str
    customer_id: str
    workflow_name: str
    started_by: str
    last_actor: Optional[str] = None
    status: str = "in_progress"  # in_progress, paused, completed, aborted
    last_node: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    steps_history: List[Dict[str, Any]] = field(default_factory=list)  # [{ts, node, actor, status}]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "InstanceMeta":
        return InstanceMeta(**d)

# ==========
# Workflow definition with Interrupt guards
# ==========
def build_claim_workflow():
    def ensure_defaults(s: Dict[str, Any]) -> None:
        s.setdefault("bag", {})
        s.setdefault("meta", {})
        s["meta"].setdefault("status", "in_progress")
        s["meta"].setdefault("start_time", now_iso())

    def validate_request(s: Dict[str, Any]):
        ensure_defaults(s)
        s["meta"]["last_node"] = "Validate Request"
        if "validate" not in s["bag"]:
            return Interrupt("Validate request? (yes/no)")
        return s

    def gather_claim_info(s: Dict[str, Any]):
        ensure_defaults(s)
        s["meta"]["last_node"] = "Gather Claim Info"
        if "claim_details" not in s["bag"]:
            return Interrupt("Provide claim details")
        return s

    def identify_accounts(s: Dict[str, Any]):
        ensure_defaults(s)
        s["meta"]["last_node"] = "Identify Accounts & Process Decision"
        if "process_decision" not in s["bag"]:
            return Interrupt("Decision? cancel / hold / suppress")
        return s

    def cancel_request(s: Dict[str, Any]):
        ensure_defaults(s)
        s["meta"]["last_node"] = "Cancel CWD Request"
        s["meta"]["status"] = "aborted"
        s["meta"]["end_time"] = now_iso()
        s["bag"]["result"] = "Workflow aborted."
        return s

    def hold_request(s: Dict[str, Any]):
        ensure_defaults(s)
        s["meta"]["last_node"] = "Hold Request"
        if "hold_action" not in s["bag"]:
            return Interrupt("Workflow on hold. Command: resume / abort")
        return s

    def apply_suppression(s: Dict[str, Any]):
        ensure_defaults(s)
        s["meta"]["last_node"] = "Apply Temporary Suppression"
        if "proceed_fulfill" not in s["bag"]:
            return Interrupt("Proceed to fulfill? (yes/no)")
        return s

    def fulfill_case(s: Dict[str, Any]):
        ensure_defaults(s)
        s["meta"]["last_node"] = "Fulfill Case and Detect"
        s["meta"]["status"] = "completed"
        s["meta"]["end_time"] = now_iso()
        s["bag"]["result"] = "Fulfilled and detection complete."
        return s

    g = StateGraph(dict)
    g.add_node("Validate Request", validate_request)
    g.add_node("Gather Claim Info", gather_claim_info)
    g.add_node("Identify Accounts & Process Decision", identify_accounts)
    g.add_node("Cancel CWD Request", cancel_request)
    g.add_node("Hold Request", hold_request)
    g.add_node("Apply Temporary Suppression", apply_suppression)
    g.add_node("Fulfill Case and Detect", fulfill_case)

    g.set_entry_point("Validate Request")

    # Conditional edges with Interrupt guards (return END when paused)
    g.add_conditional_edges(
        "Validate Request",
        lambda s: END if isinstance(s, Interrupt) else (
            "Gather Claim Info" if s["bag"].get("validate") == "yes"
            else "Cancel CWD Request" if s["bag"].get("validate") == "no"
            else END
        ),
        {"Gather Claim Info": "Gather Claim Info", "Cancel CWD Request": "Cancel CWD Request", END: END},
    )
    g.add_conditional_edges(
        "Gather Claim Info",
        lambda s: END if isinstance(s, Interrupt) else (
            "Identify Accounts & Process Decision" if s["bag"].get("claim_details") else END
        ),
        {"Identify Accounts & Process Decision": "Identify Accounts & Process Decision", END: END},
    )
    g.add_conditional_edges(
        "Identify Accounts & Process Decision",
        lambda s: END if isinstance(s, Interrupt) else (
            "Cancel CWD Request" if s["bag"].get("process_decision") == "cancel"
            else "Hold Request" if s["bag"].get("process_decision") == "hold"
            else "Apply Temporary Suppression" if s["bag"].get("process_decision") == "suppress"
            else END
        ),
        {"Cancel CWD Request": "Cancel CWD Request", "Hold Request": "Hold Request", "Apply Temporary Suppression": "Apply Temporary Suppression", END: END},
    )
    g.add_conditional_edges(
        "Hold Request",
        lambda s: END if isinstance(s, Interrupt) else (
            "Apply Temporary Suppression" if s["bag"].get("hold_action") == "resume"
            else "Cancel CWD Request" if s["bag"].get("hold_action") == "abort"
            else END
        ),
        {"Apply Temporary Suppression": "Apply Temporary Suppression", "Cancel CWD Request": "Cancel CWD Request", END: END},
    )
    g.add_conditional_edges(
        "Apply Temporary Suppression",
        lambda s: END if isinstance(s, Interrupt) else (
            "Fulfill Case and Detect" if s["bag"].get("proceed_fulfill") == "yes"
            else "Cancel CWD Request" if s["bag"].get("proceed_fulfill") == "no"
            else END
        ),
        {"Fulfill Case and Detect": "Fulfill Case and Detect", "Cancel CWD Request": "Cancel CWD Request", END: END},
    )
    g.add_edge("Fulfill Case and Detect", END)
    g.add_edge("Cancel CWD Request", END)

    return g.compile()

# ==========
# Engine for LangGraph 0.6.6
# ==========
class Engine:
    STATE_NS = "workflow_state"
    META_NS = "workflow_meta"
    EVENTS_NS = "workflow_events"
    INDEX_NS = "workflow_index"

    def __init__(self, store: InMemoryStore, workflow_name: str = "ClaimWorkflow"):
        self.store = store
        self.workflow_name = workflow_name
        self.graph = build_claim_workflow()
        #grap = self.graph
        #display(Image(grap.get_graph().draw_mermaid_png()))

    # Unwrap Item.value from store.get()
    def _unwrap(self, item):
        return item.value if item else None

    # Index helpers
    def _add_to_index(self, instance_id: str) -> None:
        idx = self._unwrap(self.store.get(self.INDEX_NS, "instances")) or []
        if instance_id not in idx:
            idx.append(instance_id)
            self.store.put(self.INDEX_NS, "instances", idx)

    # Event log
    def _append_event(
        self,
        instance_id: str,
        event: str,
        node: Optional[str],
        status: str,
        actor: Optional[str],
        data: Dict[str, Any],
    ) -> None:
        events = self._unwrap(self.store.get(self.EVENTS_NS, instance_id)) or []
        events.append({
            "ts": now_iso(),
            "instance_id": instance_id,
            "event": event,
            "node": node,
            "status": status,
            "actor": actor,
            "data": data,
        })
        self.store.put(self.EVENTS_NS, instance_id, events)

    # Meta helpers
    def _put_meta(self, m: InstanceMeta) -> None:
        self.store.put(self.META_NS, m.instance_id, m.to_dict())

    def _get_meta(self, instance_id: str) -> Optional[InstanceMeta]:
        d = self._unwrap(self.store.get(self.META_NS, instance_id))
        return InstanceMeta.from_dict(d) if d else None

    def _update_meta_from_state(self, instance_id: str, actor: str, s: Dict[str, Any]) -> InstanceMeta:
        m = self._get_meta(instance_id)
        if not m:
            raise ValueError("Missing meta")
        last_node = s.get("meta", {}).get("last_node")
        status = s.get("meta", {}).get("status", m.status)

        m.last_actor = actor
        if last_node:
            m.last_node = last_node
        if s.get("meta", {}).get("start_time") and not m.start_time:
            m.start_time = s["meta"]["start_time"]
        if s.get("meta", {}).get("end_time"):
            m.end_time = s["meta"]["end_time"]
        m.status = status

        # Step history on node change
        prev_node = m.steps_history[-1]["node"] if m.steps_history else None
        if last_node and last_node != prev_node:
            m.steps_history.append({
                "ts": now_iso(),
                "node": last_node,
                "actor": actor,
                "status": m.status
            })

        self._put_meta(m)
        return m

    # ---- lifecycle ----
    def start(self, customer_id: str, started_by: str) -> Tuple[str, Dict[str, Any]]:
        instance_id = str(uuid.uuid4())
        # Initial graph state
        state = {
            "instance_id": instance_id,
            "customer_id": customer_id,
            "workflow_name": self.workflow_name,
            "bag": {},
            "meta": {"status": "in_progress", "start_time": now_iso()},
        }
        # Audit meta
        meta = InstanceMeta(
            instance_id=instance_id,
            customer_id=customer_id,
            workflow_name=self.workflow_name,
            started_by=started_by,
            last_actor=started_by,
            status="in_progress",
            start_time=state["meta"]["start_time"],
        )

        # Persist and index
        self.store.put(self.STATE_NS, instance_id, state)
        self._put_meta(meta)
        self._add_to_index(instance_id)
        self._append_event(instance_id, "created", None, meta.status, started_by, {"customer_id": customer_id})

        # First run
        return instance_id, self._run(instance_id, state, actor=started_by)

    def resume(self, instance_id: str, actor: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        item = self.store.get(self.STATE_NS, instance_id)
        state = self._unwrap(item)
        if not state:
            raise ValueError(f"No workflow found: {instance_id}")

        # Apply HITL updates
        bag = state.setdefault("bag", {})
        bag.update(updates)

        # Persist before invoke and log
        self.store.put(self.STATE_NS, instance_id, state)
        self._append_event(
            instance_id,
            "resume_command",
            state.get("meta", {}).get("last_node"),
            "in_progress",
            actor,
            {"updates": updates},
        )
        return self._run(instance_id, state, actor=actor)
    
    def _run(self, instance_id: str, state: Dict[str, Any], actor: str) -> Dict[str, Any]:
        def update_and_return(payload: Dict[str, Any], state_for_meta: Dict[str, Any]):
            meta = self._update_meta_from_state(instance_id, actor, state_for_meta)
            self._put_meta(meta)
            return payload

        result = self.graph.invoke(state)

        # Paused (Interrupt)
        if isinstance(result, Interrupt):
            self.store.put(self.STATE_NS, instance_id, state)
            # Ensure last_node is set for diagram
            if not state.get("meta", {}).get("last_node"):
                state.setdefault("meta", {})["last_node"] = "Validate Request"
            meta = self._update_meta_from_state(instance_id, actor, state)
            if meta.status not in {"completed", "aborted"}:
                meta.status = "paused"
                self._put_meta(meta)
            self._append_event(
                instance_id,
                "paused",
                state["meta"]["last_node"],
                meta.status,
                actor,
                {"prompt": result.value},
            )
            return update_and_return({
                "status": "paused",
                "prompt": result.value,
                "instance_id": instance_id
            }, state)

        if not isinstance(result, dict):
            raise TypeError("Unexpected graph result; expected dict or Interrupt")

        self.store.put(self.STATE_NS, instance_id, result)
        if not result.get("meta", {}).get("last_node"):
            result.setdefault("meta", {})["last_node"] = "Validate Request"
        meta = self._update_meta_from_state(instance_id, actor, result)
        node = result["meta"]["last_node"]
        status = result.get("meta", {}).get("status", meta.status)

        if status == "completed":
            meta.status = "completed"
            self._put_meta(meta)
            self._append_event(
                instance_id, "completed", node, meta.status, actor,
                {"result": result.get("bag", {}).get("result")}
            )
            return update_and_return({
                "status": "completed",
                "node": node,
                "result": result.get("bag", {}).get("result"),
                "instance_id": instance_id,
            }, result)

        if status == "aborted":
            meta.status = "aborted"
            self._put_meta(meta)
            self._append_event(
                instance_id, "aborted", node, meta.status, actor,
                {"result": result.get("bag", {}).get("result")}
            )
            return update_and_return({
                "status": "aborted",
                "node": node,
                "result": result.get("bag", {}).get("result"),
                "instance_id": instance_id,
            }, result)

        meta.status = "in_progress"
        self._put_meta(meta)
        self._append_event(instance_id, "progressed", node, meta.status, actor, {})
        return update_and_return({
            "status": "in_progress",
            "node": node,
            "instance_id": instance_id
        }, result)

    # ---- run helpers ----
    # def _run(self, instance_id: str, state: Dict[str, Any], actor: str) -> Dict[str, Any]:
    #     result = self.graph.invoke(state)

    #     # Paused (Interrupt): persist and report
    #     if isinstance(result, Interrupt):
    #         self.store.put(self.STATE_NS, instance_id, state)
    #         meta = self._update_meta_from_state(instance_id, actor, state)
    #         if meta.status not in {"completed", "aborted"}:
    #             meta.status = "paused"
    #             self._put_meta(meta)
    #         self._append_event(
    #             instance_id,
    #             "paused",
    #             state.get("meta", {}).get("last_node"),
    #             meta.status,
    #             actor,
    #             {"prompt": result.value},
    #         )
    #         return {"status": "paused", "prompt": result.value, "instance_id": instance_id}

    #     # Otherwise, result must be a dict (updated state)
    #     if not isinstance(result, dict):
    #         raise TypeError("Unexpected graph result; expected dict or Interrupt")

    #     # Persist and audit
    #     self.store.put(self.STATE_NS, instance_id, result)
    #     meta = self._update_meta_from_state(instance_id, actor, result)
    #     node = result.get("meta", {}).get("last_node")
    #     status = result.get("meta", {}).get("status", meta.status)

    #     if status == "completed":
    #         meta.status = "completed"
    #         self._put_meta(meta)
    #         self._append_event(
    #             instance_id, "completed", node, meta.status, actor,
    #             {"result": result.get("bag", {}).get("result")}
    #         )
    #         return {
    #             "status": "completed",
    #             "node": node,
    #             "result": result.get("bag", {}).get("result"),
    #             "instance_id": instance_id,
    #         }

    #     if status == "aborted":
    #         meta.status = "aborted"
    #         self._put_meta(meta)
    #         self._append_event(
    #             instance_id, "aborted", node, meta.status, actor,
    #             {"result": result.get("bag", {}).get("result")}
    #         )
    #         return {
    #             "status": "aborted",
    #             "node": node,
    #             "result": result.get("bag", {}).get("result"),
    #             "instance_id": instance_id,
    #         }

    #     # Progressed without pausing
    #     meta.status = "in_progress"
    #     self._put_meta(meta)
    #     self._append_event(instance_id, "progressed", node, meta.status, actor, {})
    #     return {"status": "in_progress", "node": node, "instance_id": instance_id}

    # ---- queries ----
    def get_state(self, instance_id: str) -> Optional[Dict[str, Any]]:
        return self._unwrap(self.store.get(self.STATE_NS, instance_id))

    def get_meta(self, instance_id: str) -> Optional[InstanceMeta]:
        return self._get_meta(instance_id)

    def history(self, instance_id: str) -> List[Dict[str, Any]]:
        return self._unwrap(self.store.get(self.EVENTS_NS, instance_id)) or []

    def list_instances(
        self,
        customer_id: Optional[str] = None,
        status: Optional[str] = None,
        started_by: Optional[str] = None,
        workflow_name: Optional[str] = None,
    ) -> List[InstanceMeta]:
        ids = self._unwrap(self.store.get(self.INDEX_NS, "instances")) or []
        out: List[InstanceMeta] = []
        for iid in ids:
            m = self._get_meta(iid)
            if not m:
                continue
            if workflow_name and m.workflow_name != workflow_name:
                continue
            if customer_id and m.customer_id != customer_id:
                continue
            if status and m.status != status:
                continue
            if started_by and m.started_by != started_by:
                continue
            out.append(m)
        out.sort(key=lambda x: (x.steps_history[-1]["ts"] if x.steps_history else ""), reverse=True)
        return out

# ==========
# Demo
# ==========
if __name__ == "__main__":
    store = InMemoryStore()
    engine = Engine(store, workflow_name="ClaimWorkflow")

    # Start
    inst_id, start_out = engine.start(customer_id="C1", started_by="user_a")
    print("Start:", start_out)

    # Validate
    out = engine.resume(inst_id, actor="user_b", updates={"validate": "yes"})
    print("After validate:", out)

    # Provide claim details
    out = engine.resume(inst_id, actor="user_c", updates={"claim_details": "Claim for disputed withdrawal"})
    print("After claim info:", out)

    # Decide to hold
    out = engine.resume(inst_id, actor="user_d", updates={"process_decision": "hold"})
    print("On hold:", out)

    # Resume from hold
    out = engine.resume(inst_id, actor="user_e", updates={"hold_action": "resume"})
    print("Resumed:", out)

    # Proceed to fulfill
    out = engine.resume(inst_id, actor="user_f", updates={"proceed_fulfill": "yes"})
    print("Completed:", out)

    # Inspect history and meta
    print("\nEvents:")
    for ev in engine.history(inst_id):
        print("  ", ev)

    print("\nMeta:", engine.get_meta(inst_id))