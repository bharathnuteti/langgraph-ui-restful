# api.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from workflow_backend import engine

class StepInput(BaseModel):
    actor: str
    updates: dict

app = FastAPI()

@app.post("/workflow/{instance_id}/human-step")
def provide_human_input(instance_id: str, step_input: StepInput):
    meta = engine.get_meta(instance_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Workflow not found")

    allowed_steps = {"Gather Claim Info", "Validate Request"}
    if meta.last_node not in allowed_steps:
        raise HTTPException(status_code=400, detail=f"Current step '{meta.last_node}' is not allowed via API")

    result = engine.resume(instance_id, step_input.actor, step_input.updates)
    return {"instance_id": instance_id, "result": result}

@app.get("/workflow/pending-human")
def pending_human_steps():
    allowed_steps = {"Gather Claim Info", "Validate Request"}
    metas = engine.list_instances(status="paused")
    return [m.to_dict() for m in metas if m.last_node in allowed_steps]