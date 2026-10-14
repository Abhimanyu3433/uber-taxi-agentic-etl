# Databricks notebook source
# MAGIC %md
# MAGIC # Microsoft Foundry GPT - connection test
# MAGIC (These cells can also be pasted at the end of `setup.ipynb`, which the spec names for this test.)
# MAGIC
# MAGIC **Before running:**
# MAGIC 1. A GPT model is deployed in Microsoft Foundry (README → "Phase 2 setup").
# MAGIC 2. Key Vault `kv-uber-etl-abhi01` contains two extra secrets:
# MAGIC    `foundry-endpoint` (e.g. `https://<resource>.openai.azure.com/`) and `foundry-deployment` (your deployment name).
# MAGIC 3. The service principal `sp-uber-etl` has the role **Cognitive Services OpenAI User** on the Foundry resource
# MAGIC    (Entra ID authentication - no API key needed).

# COMMAND ----------

from src import config
from src.agents import FoundryClient, mask_sensitive

llm = FoundryClient(spark)
print("Endpoint:", llm.endpoint or "(not set)")
print("Deployment:", llm.deployment or "(not set)")
print("Client ready:", llm.client is not None, "| error:", llm.error)

# COMMAND ----------

# Simple structured prompt (the agents always ask for JSON so decisions are machine-readable)
answer = llm.chat_json(
    "You are a test assistant.",
    {"task": "Reply with {\"status\": \"ok\", \"model_says\": \"<one short greeting>\"}"},
    max_tokens=60)
print("Answer:", answer)
print("Tokens used:", llm.tokens_used, "| last error:", llm.error)
assert answer and answer.get("status") == "ok", "Foundry test failed - see error above"
print("✓ Foundry GPT reachable with Entra ID authentication")   # 📸 screenshot

# COMMAND ----------

# Guardrail demo: IDs are masked before any prompt leaves the cluster
print(mask_sensitive('booking CNR5884300 by customer CID1982111'))
