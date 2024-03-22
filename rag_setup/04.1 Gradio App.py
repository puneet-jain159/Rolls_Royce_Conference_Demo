# Databricks notebook source
# MAGIC %pip install dbtunnel[gradio,ngrok] gradio

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.18.0 mlflow==2.10.1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-init-advanced $reset_all_data=false

# COMMAND ----------

import urllib
import json
import mlflow

mlflow.set_registry_uri('databricks-uc')
client = MlflowClient()
model_name = f"{catalog}.{db}.dbdemos_advanced_chatbot_model"
serving_endpoint_name = f"dbdemos_endpoint_advanced_{catalog}_{db}"[:63]
latest_model = client.get_model_version_by_alias(model_name, "prod")

serving_client = EndpointApiClient()

# COMMAND ----------


serving_client.query_inference_endpoint(
    serving_endpoint_name,
    {
    "messages": [
        {"role": "user", "content": "What was the engine model??"}, 
        {"role": "assistant", "content": "Pratt &amp; Whitney PW4077 turbo fan engine."}, 
        {"role": "user", "content": "who is the manufacturer?"}
    ]
    },
)

# COMMAND ----------

def generate_dialogue(chat_history):
  dialog = {"messages":[]}
  for chat in chat_history:
    dialog['messages'].append({"role": "user", "content": chat[0]})
    dialog['messages'].append({"role": "assistant", "content": chat[1]})
  return dialog

def get_response(dialog):
  response = serving_client.query_inference_endpoint(
    serving_endpoint_name,
    dialog)
  return response[0]['result']

# COMMAND ----------

import gradio as gr
import os
import time


with gr.Blocks() as demo:
    gr.Markdown("""<img align="right" src="https://getintoteaching.education.gov.uk/packs/v1/static/images/logo/teaching_black_background_pink_underline-489832e6b07fd47b67d1.svg" alt="logo" width="120" >

## A Retrieval Augmented Generation (RAG) demo to understand the Aviation Investigation into United Airlines flight 328.

#### Questions are compared against Final Investigation Report to find which paragraphs are the most relevant. The LLM then uses these pages as the basis for it's answer.

""")
    
    chatbot=gr.Chatbot(height="70%")
    msg = gr.Textbox(label="Question")
    clear = gr.ClearButton([msg, chatbot])
    examples = gr.Examples(examples=["Summarise the incident in a paragraph?", "What was the engine model and engine manufacturer involved in the incident?","Describe the aircraft involved in the incident?"],
                        inputs=[msg])

    def respond(msg ,chat_history):
        dialog = generate_dialogue(chat_history)
        dialog['messages'].append({"role": "user", "content": msg})
        response = get_response(dialog)
        chat_history.append((msg, response))
        return "", chat_history

    msg.submit(respond, [msg, chatbot], [msg, chatbot])

def same_auth(username, password):
    return username == password

demo.launch(auth=same_auth)
demo.queue()

# COMMAND ----------

from dbtunnel import dbtunnel
dbtunnel.gradio(demo).share_to_internet_via_ngrok(
    ngrok_api_token="<Add API>",
    ngrok_tunnel_auth_token="<Add API>"
).run()

# COMMAND ----------

dbtunnel.kill_port(8080)

# COMMAND ----------


