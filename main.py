import os
import sys
import time
import re
import json
import requests
import logging
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCleaner:
    @staticmethod
    def clean_text(text):
        """Очищает текст от HTML-тегов и системных символов."""
        if not text:
            return ""
        
        # Используем BeautifulSoup для удаления HTML тегов
        soup = BeautifulSoup(text, "html.parser")
        clean_text = soup.get_text(separator=" ")
        
        # Удаляем лишние пробелы и непечатаемые символы
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        
        return clean_text


def extract_text_payload(params, fallback_text):
    if not isinstance(params, dict):
        params = {}
    return params.get("text") or fallback_text or ""


def clean_note_payload(note, cleaner):
    params = note.get("params", {}) or {}
    note_type = note.get("note_type")
    clean_text = cleaner.clean_text(extract_text_payload(params, note.get("text")))

    call_link = None
    call_duration = None
    call_status = None
    call_phone = None
    call_source = None
    is_call = False
    is_chat = False

    if note_type in ["call_in", "call_out"]:
        is_call = True
        call_link = params.get("link")
        try:
            call_duration = int(params.get("duration", 0)) if params.get("duration") is not None else 0
        except (ValueError, TypeError):
            call_duration = 0
        call_status = str(params.get("call_status")) if params.get("call_status") is not None else None
        call_phone = params.get("phone")
        call_source = params.get("source")
    elif note_type in ["message_in", "message_out", "talk_incoming", "talk_outgoing"]:
        is_chat = True

    return {
        "id": str(note.get("id")),
        "entity_id": str(note.get("entity_id")),
        "entity_type": note.get("entity_type"),
        "note_type": note_type,
        "responsible_user_id": str(note.get("responsible_user_id")),
        "created_by": str(note.get("created_by")),
        "updated_by": str(note.get("updated_by")),
        "created_at": note.get("created_at"),
        "updated_at": note.get("updated_at"),
        "account_id": str(note.get("account_id")),
        "text": clean_text,
        "group_id": str(note.get("group_id")),
        "call_link": call_link,
        "call_duration": call_duration,
        "call_status": call_status,
        "call_phone": call_phone,
        "call_source": call_source,
        "is_call": is_call,
        "is_chat": is_chat,
    }


def clean_task_payload(task, cleaner):
    result_data = task.get("result")
    result_text = ""
    if isinstance(result_data, dict):
        result_text = result_data.get("text", "")
    elif isinstance(result_data, list) and result_data:
        first_item = result_data[0]
        if isinstance(first_item, dict):
            result_text = first_item.get("text", "")
        else:
            result_text = str(first_item)

    return {
        "id": str(task.get("id")),
        "entity_id": str(task.get("entity_id")),
        "entity_type": task.get("entity_type"),
        "responsible_user_id": str(task.get("responsible_user_id")),
        "group_id": str(task.get("group_id")),
        "duration": task.get("duration"),
        "is_completed": task.get("is_completed"),
        "task_type_id": str(task.get("task_type_id")),
        "text": cleaner.clean_text(task.get("text") or ""),
        "result_text": cleaner.clean_text(result_text),
        "created_by": str(task.get("created_by")),
        "updated_by": str(task.get("updated_by")),
        "created_at": task.get("created_at"),
        "updated_at": task.get("updated_at"),
        "complete_till": task.get("complete_till"),
        "u_status": task.get("u_status"),
        "account_id": str(task.get("account_id")),
    }


def extract_event_field(entries, section, field):
    for entry in entries or []:
        section_data = entry.get(section) or {}
        value = section_data.get(field)
        if value is not None:
            return str(value)
    return None


def extract_linked_entity(entries):
    for entry in entries or []:
        link = entry.get("link") or {}
        entity = link.get("entity") or {}
        entity_id = entity.get("id")
        entity_type = entity.get("type")
        if entity_id is not None or entity_type is not None:
            return (
                str(entity_type) if entity_type is not None else None,
                str(entity_id) if entity_id is not None else None,
            )
    return None, None


def clean_event_payload(event):
    value_before = event.get("value_before") or []
    value_after = event.get("value_after") or []
    linked_entity_type, linked_entity_id = extract_linked_entity(value_after or value_before)

    return {
        "id": str(event.get("id")),
        "entity_id": str(event.get("entity_id")),
        "entity_type": event.get("entity_type"),
        "created_at": event.get("created_at"),
        "created_by": str(event.get("created_by")),
        "type": event.get("type"),
        "value_raw": json.dumps(value_before, ensure_ascii=False),
        "value_clean": json.dumps(value_after, ensure_ascii=False),
        "account_id": str(event.get("account_id")),
        "value_before_json": json.dumps(value_before, ensure_ascii=False),
        "value_after_json": json.dumps(value_after, ensure_ascii=False),
        "lead_status_before_id": extract_event_field(value_before, "lead_status", "id"),
        "lead_status_after_id": extract_event_field(value_after, "lead_status", "id"),
        "lead_status_before_pipeline_id": extract_event_field(value_before, "lead_status", "pipeline_id"),
        "lead_status_after_pipeline_id": extract_event_field(value_after, "lead_status", "pipeline_id"),
        "note_id": extract_event_field(value_after, "note", "id") or extract_event_field(value_before, "note", "id"),
        "tag_name": extract_event_field(value_after, "tag", "name") or extract_event_field(value_before, "tag", "name"),
        "message_id": extract_event_field(value_after, "message", "id") or extract_event_field(value_before, "message", "id"),
        "linked_entity_type": linked_entity_type,
        "linked_entity_id": linked_entity_id,
        "responsible_user_before_id": extract_event_field(value_before, "responsible_user", "id"),
        "responsible_user_after_id": extract_event_field(value_after, "responsible_user", "id"),
    }

class KommoClient:
    def __init__(self, subdomain, access_token):
        self.base_url = f"https://{subdomain}.kommo.com/api/v4"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        })

    def _get_request(self, endpoint, params=None):
        """Выполняет GET запрос с обработкой Rate Limiting и тайм-аутами."""
        url = f"{self.base_url}/{endpoint}"
        retries = 3
        
        while retries > 0:
            try:
                # Adding explicit timeout to prevent hanging connections
                response = self.session.get(url, params=params, timeout=60)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 204:
                    return None
                elif response.status_code == 429:
                    logger.warning("Rate limit exceeded. Waiting for 2 seconds...")
                    time.sleep(2)
                    retries -= 1
                    continue
                else:
                    logger.error(f"Error {response.status_code}: {response.text}")
                    return None
            except requests.RequestException as e:
                logger.error(f"Request failed: {e}")
                retries -= 1
                time.sleep(2)
        
        return None

    def get_leads_with_filter(self, date_from, pipeline_ids):
        """
        Retrieves leads created after date_from and belonging to specific pipelines.
        """
        all_leads = []
        page = 1
        limit = 250
        
        params = {
            "limit": limit,
            "with": "custom_fields,contacts",
            "filter[created_at][from]": date_from
        }
        
        for i, pid in enumerate(pipeline_ids):
            params[f"filter[pipeline_id][{i}]"] = pid
            
        logger.info(f"Fetching leads created after timestamp {date_from} in pipelines {pipeline_ids}...")

        while True:
            params["page"] = page
            data = self._get_request("leads", params=params)
            
            if not data:
                break
                
            embedded = data.get("_embedded", {})
            leads = embedded.get("leads", [])
            
            if not leads:
                break
                
            all_leads.extend(leads)
            logger.info(f"Fetched {len(leads)} leads (Page {page})")
            
            page += 1
            if len(leads) < limit:
                break
                
        return all_leads

    def get_successful_leads(self, pipeline_ids):
        """
        Retrieves all leads with status 142 (Won) from specific pipelines, for all time.
        """
        all_leads = []
        page = 1
        limit = 250
        
        params = {
            "limit": limit,
            "with": "custom_fields,contacts",
            "filter[status][0]": 142
        }
        
        for i, pid in enumerate(pipeline_ids):
            params[f"filter[pipeline_id][{i}]"] = pid
            
        logger.info(f"Fetching ALL successful leads (Status 142) in pipelines {pipeline_ids}...")

        while True:
            params["page"] = page
            data = self._get_request("leads", params=params)
            
            if not data:
                break
                
            embedded = data.get("_embedded", {})
            leads = embedded.get("leads", [])
            
            if not leads:
                break
                
            all_leads.extend(leads)
            logger.info(f"Fetched {len(leads)} successful leads (Page {page})")
            
            page += 1
            if len(leads) < limit:
                break
                
        return all_leads

    def _get_entities_paginated(self, endpoint, entity_ids, params, result_key, entity_label, batch_size=50):
        all_items = []
        unique_ids = [str(entity_id) for entity_id in sorted({str(entity_id) for entity_id in entity_ids})]
        total_entities = len(unique_ids)

        if not unique_ids:
            return all_items

        logger.info(f"Fetching {endpoint} for {total_entities} {entity_label} in batches of {batch_size}...")

        for i in range(0, total_entities, batch_size):
            batch_ids = unique_ids[i:i + batch_size]
            request_params = dict(params)
            for j, entity_id in enumerate(batch_ids):
                request_params[f"filter[entity_id][{j}]"] = entity_id

            try:
                page = 1
                while True:
                    request_params["page"] = page
                    data = self._get_request(endpoint, params=request_params)
                    if not data:
                        break

                    embedded = data.get("_embedded", {})
                    items = embedded.get(result_key, [])
                    if not items:
                        break

                    all_items.extend(items)
                    if len(items) < request_params.get("limit", 250):
                        break
                    page += 1

                processed = min(i + batch_size, total_entities)
                if processed % 250 == 0 or processed >= total_entities:
                    logger.info(f"Processed {endpoint} for {processed}/{total_entities} {entity_label}...")
            except Exception as e:
                logger.error(f"Error fetching {endpoint} for batch starting at index {i}: {e}")

        return all_items

    def get_notes_for_entities(self, entity_ids, entity_type):
        endpoint = "leads/notes" if entity_type == "leads" else "contacts/notes"
        params = {
            "limit": 250,
            "filter[entity_type]": entity_type,
        }
        return self._get_entities_paginated(endpoint, entity_ids, params, "notes", entity_type)

    def get_tasks_for_entities(self, entity_ids, entity_type):
        params = {
            "limit": 250,
            "filter[entity_type]": entity_type,
        }
        return self._get_entities_paginated("tasks", entity_ids, params, "tasks", entity_type)

    def get_events_for_entities(self, entity_ids, entity_type, created_from=None):
        params = {
            "limit": 250,
            "filter[entity]": entity_type,
        }
        if created_from is not None:
            params["filter[created_at][from]"] = created_from
        return self._get_entities_paginated("events", entity_ids, params, "events", entity_type, batch_size=10)

class BigQueryLoader:
    def __init__(self, project_id, dataset_id, credentials_path):
        self.client = bigquery.Client.from_service_account_json(credentials_path)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.dataset_ref = f"{project_id}.{dataset_id}"
        self._create_dataset_if_not_exists()

    def _create_dataset_if_not_exists(self):
        try:
            self.client.get_dataset(self.dataset_ref)
            logger.info(f"Dataset {self.dataset_ref} exists.")
        except Exception:
            logger.info(f"Creating dataset {self.dataset_ref}...")
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Dataset {self.dataset_ref} created.")

    def ensure_table(self, table_name, schema):
        table_ref = f"{self.dataset_ref}.{table_name}"
        table = bigquery.Table(table_ref, schema=schema)
        try:
            self.client.delete_table(table_ref, not_found_ok=True)
            self.client.create_table(table)
            logger.info(f"Reset table {table_ref} with explicit schema.")
        except Exception as e:
            logger.error(f"Failed to reset table {table_ref}: {e}")
            raise

    def upload_data(self, table_name, data, schema):
        if not data:
            self.ensure_table(table_name, schema)
            logger.info(f"No data to upload for table {table_name}. Table schema was refreshed.")
            return

        table_ref = f"{self.dataset_ref}.{table_name}"
        
        # Split data into chunks of 2000 rows to prevent memory spikes and large request errors
        chunk_size = 2000
        write_disposition = "WRITE_TRUNCATE"
        
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=write_disposition,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            
            try:
                job = self.client.load_table_from_json(chunk, table_ref, job_config=job_config)
                job.result()  # Wait for completion
                logger.info(f"Loaded {len(chunk)} rows into {table_ref} (Mode: {write_disposition}).")
                
                # After the first chunk, switch to APPEND to preserve previous chunks
                write_disposition = "WRITE_APPEND"
            except Exception as e:
                logger.error(f"Failed to upload chunk starting at {i} to {table_name}: {e}")
                if hasattr(e, 'errors'):
                     logger.error(f"Errors: {e.errors}")
                raise e # Fail fast if upload fails

    def execute_query(self, query):
        """Executes a SQL query (for post-sync dashboard refresh)."""
        try:
            logger.info("Executing post-sync SQL update for dashboard...")
            job = self.client.query(query)
            job.result()
            logger.info("SQL update completed successfully.")
        except Exception as e:
            logger.error(f"SQL update failed: {e}")


# ------------------------------------------------------------------------------
# Cloud Run / Functions Framework Entry Point
# ------------------------------------------------------------------------------
from flask import Flask, request

app = Flask(__name__)

@app.route("/", methods=["POST", "GET"]) 
def handle_request(request=None):
    """
    HTTP Cloud Function Entry Point.
    """
    try:
        logger.info("Triggered from HTTP request.")
        main_logic()
        return "Success"
    except Exception as e:
        logger.error(f"Function failed: {e}")
        return f"Error: {e}"

def main_logic():
    load_dotenv()

    # Configuration
    SUBDOMAIN = os.getenv("KOMMO_SUBDOMAIN")
    ACCESS_TOKEN = os.getenv("KOMMO_ACCESS_TOKEN")
    BQ_PROJECT = os.getenv("BIGQUERY_PROJECT_ID")
    BQ_DATASET = os.getenv("BIGQUERY_DATASET_ID")
    BQ_CREDS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "service_account.json"
    
    # Tables
    TABLE_LEADS = "leads"
    TABLE_NOTES = "notes"
    TABLE_TASKS = "tasks"
    TABLE_CONTACT_NOTES = "contact_notes"
    TABLE_CONTACT_TASKS = "contact_tasks"
    TABLE_CRM_EVENTS = "crm_events"
    TABLE_LEAD_CONTACT_LINKS = "lead_contact_links"

    if not all([SUBDOMAIN, ACCESS_TOKEN, BQ_PROJECT, BQ_DATASET]):
        logger.error("Missing configuration in .env file.")
        # In Cloud Run context, we might not want to exit the process, just raise error
        if not os.getenv("PORT"): # Local run
             sys.exit(1)
        else:
             raise ValueError("Missing configuration in .env file.")

    # 1. Pipeline and Date Configuration
    # Pipelines: Сделки Бали (6446851), Лиды Бали (7497175), Europe Deals (8382455), 
    # Leads Europe (8382227), Leads Thailand (11289648), Thailand Deals (11289652), 
    # Вебинары (8445171), Лиды Таиланд (10497554), Сделки Таиланд (10497558), Техническая (7544327)
    TARGET_PIPELINES = [6446851, 7497175, 8382455, 8382227, 11289648, 11289652, 8445171, 10497554, 10497558, 7544327]
    START_DATE = datetime(2026, 1, 1) # Jan 1, 2026
    TIMESTAMP_FROM = int(START_DATE.timestamp())

    logger.info(f"--- Starting Export ---")
    logger.info(f"Date Filter: > {START_DATE.isoformat()} (Timestamp: {TIMESTAMP_FROM})")
    logger.info(f"Target Pipelines: {TARGET_PIPELINES}")

    kommo = KommoClient(SUBDOMAIN, ACCESS_TOKEN)
    bq = BigQueryLoader(BQ_PROJECT, BQ_DATASET, BQ_CREDS)
    cleaner = DataCleaner()

    # 2. Fetch LEADS
    logger.info("Step 1a: Fetching New Leads (> 01.01.2026)...")
    raw_leads_new = kommo.get_leads_with_filter(TIMESTAMP_FROM, TARGET_PIPELINES)
    
    logger.info("Step 1b: Fetching Historic Successful Leads (Status 142)...")
    raw_leads_won = kommo.get_successful_leads(TARGET_PIPELINES)
    
    # Merge leads, avoiding duplicates
    leads_map = {}
    
    for lead in raw_leads_new:
        leads_map[lead['id']] = lead
        
    for lead in raw_leads_won:
        leads_map[lead['id']] = lead
        
    merged_leads = list(leads_map.values())
    eligible_lead_ids = set(leads_map.keys())
    
    logger.info(f"Total Combined Leads: {len(merged_leads)} (New: {len(raw_leads_new)}, Won: {len(raw_leads_won)})")

    cleaned_leads = []
    contact_link_rows = []
    contact_ids = set()
    
    for lead in merged_leads:
        lead_id = lead.get("id")
        embedded_contacts = ((lead.get("_embedded") or {}).get("contacts") or [])

        for idx, contact in enumerate(embedded_contacts):
            contact_id = contact.get("id")
            if contact_id is None:
                continue
            contact_id = str(contact_id)
            contact_ids.add(contact_id)
            contact_link_rows.append({
                "lead_id": str(lead_id),
                "contact_id": contact_id,
                "is_primary": idx == 0,
                "link_source": "lead_embedded",
            })
        
        # Extract UTMs and GCLID from custom fields
        custom_fields = lead.get("custom_fields_values", []) or []
        utm_data = {
            "utm_source": None,
            "utm_campaign": None,
            "utm_medium": None,
            "utm_term": None,
            "utm_content": None,
            "gclid": None
        }
        
        field_mapping = {
            "utm_source": "utm_source",
            "utm source": "utm_source",
            "utm_campaign": "utm_campaign",
            "utm campaign": "utm_campaign",
            "utm_medium": "utm_medium",
            "utm medium": "utm_medium",
            "utm_term": "utm_term",
            "utm term": "utm_term",
            "utm_content": "utm_content",
            "utm content": "utm_content",
            "gclid": "gclid"
        }

        for cf in custom_fields:
            cf_name = cf.get("field_name", "").lower().strip()
            if cf_name in field_mapping:
                vals = cf.get("values", [])
                if vals:
                    utm_data[field_mapping[cf_name]] = vals[0].get("value")

        cleaned_lead = {
            "id": str(lead_id),
            "name": lead.get("name"),
            "price": lead.get("price"),
            "responsible_user_id": str(lead.get("responsible_user_id")),
            "group_id": str(lead.get("group_id")),
            "status_id": str(lead.get("status_id")),
            "pipeline_id": str(lead.get("pipeline_id")),
            "loss_reason_id": str(lead.get("loss_reason_id")),
            "created_by": str(lead.get("created_by")),
            "updated_by": str(lead.get("updated_by")),
            "created_at": lead.get("created_at"), # Timestamp
            "updated_at": lead.get("updated_at"), # Timestamp
            "closed_at": lead.get("closed_at"),   # Timestamp
            "closest_task_at": lead.get("closest_task_at"),
            "is_deleted": lead.get("is_deleted"),
            "score": lead.get("score"),
            "account_id": str(lead.get("account_id")),
            "labor_cost": lead.get("labor_cost"),
            # UTMs
            "utmSource": utm_data["utm_source"],
            "utmCampaign": utm_data["utm_campaign"],
            "utmMedium": utm_data["utm_medium"],
            "utmTerm": utm_data["utm_term"],
            "utmContent": utm_data["utm_content"],
            "ad_id": utm_data["gclid"] # Mapping gclid to ad_id as per dashboard requirements
        }
        cleaned_leads.append(cleaned_lead)

    deduped_links = {}
    for row in contact_link_rows:
        deduped_links[(row["lead_id"], row["contact_id"])] = row
    contact_link_rows = list(deduped_links.values())

    target_lead_ids_list = list(eligible_lead_ids)
    target_contact_ids_list = sorted(contact_ids)

    # 3. Fetch lead-side activity
    logger.info("Step 2: Fetching lead notes...")
    raw_notes = kommo.get_notes_for_entities(target_lead_ids_list, "leads")
    cleaned_notes = [clean_note_payload(note, cleaner) for note in raw_notes]
    logger.info(f"Total Lead Notes Fetched: {len(raw_notes)}")

    logger.info("Step 3: Fetching lead tasks...")
    raw_tasks = kommo.get_tasks_for_entities(target_lead_ids_list, "leads")
    cleaned_tasks = [clean_task_payload(task, cleaner) for task in raw_tasks]
    logger.info(f"Total Lead Tasks Fetched: {len(raw_tasks)}")

    # 4. Fetch contact-side activity into separate tables so Polina's current reports stay unchanged
    logger.info("Step 4: Fetching contact notes...")
    raw_contact_notes = kommo.get_notes_for_entities(target_contact_ids_list, "contacts")
    cleaned_contact_notes = [clean_note_payload(note, cleaner) for note in raw_contact_notes]
    logger.info(f"Total Contact Notes Fetched: {len(raw_contact_notes)}")

    logger.info("Step 5: Fetching contact tasks...")
    raw_contact_tasks = kommo.get_tasks_for_entities(target_contact_ids_list, "contacts")
    cleaned_contact_tasks = [clean_task_payload(task, cleaner) for task in raw_contact_tasks]
    logger.info(f"Total Contact Tasks Fetched: {len(raw_contact_tasks)}")

    # 5. Fetch events for stage history and enriched audit joins
    logger.info("Step 6: Fetching CRM events for leads and contacts...")
    raw_lead_events = kommo.get_events_for_entities(target_lead_ids_list, "lead", created_from=TIMESTAMP_FROM)
    raw_contact_events = kommo.get_events_for_entities(target_contact_ids_list, "contact", created_from=TIMESTAMP_FROM)
    cleaned_events = [clean_event_payload(event) for event in raw_lead_events + raw_contact_events]
    logger.info(
        f"Total CRM Events Fetched: {len(cleaned_events)} "
        f"(Lead: {len(raw_lead_events)}, Contact: {len(raw_contact_events)})"
    )


    # 5. Upload to BigQuery (WRITE_TRUNCATE is handled in upload_data)
    logger.info("Step 4: Uploading to BigQuery...")
    
    # Leads Schema
    schema_leads = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("price", "INTEGER"),
        bigquery.SchemaField("responsible_user_id", "STRING"),
        bigquery.SchemaField("group_id", "STRING"),
        bigquery.SchemaField("status_id", "STRING"),
        bigquery.SchemaField("pipeline_id", "STRING"),
        bigquery.SchemaField("loss_reason_id", "STRING"),
        bigquery.SchemaField("created_by", "STRING"),
        bigquery.SchemaField("updated_by", "STRING"),
        bigquery.SchemaField("created_at", "INTEGER"),
        bigquery.SchemaField("updated_at", "INTEGER"),
        bigquery.SchemaField("closed_at", "INTEGER"),
        bigquery.SchemaField("closest_task_at", "INTEGER"),
        bigquery.SchemaField("is_deleted", "BOOLEAN"),
        bigquery.SchemaField("score", "INTEGER"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("labor_cost", "INTEGER"),
        bigquery.SchemaField("utmSource", "STRING"),
        bigquery.SchemaField("utmCampaign", "STRING"),
        bigquery.SchemaField("utmMedium", "STRING"),
        bigquery.SchemaField("utmTerm", "STRING"),
        bigquery.SchemaField("utmContent", "STRING"),
        bigquery.SchemaField("ad_id", "STRING"),
    ]
    
    # Notes Schema
    schema_notes = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("entity_id", "STRING"),
        bigquery.SchemaField("entity_type", "STRING"),
        bigquery.SchemaField("note_type", "STRING"),
        bigquery.SchemaField("responsible_user_id", "STRING"),
        bigquery.SchemaField("created_by", "STRING"),
        bigquery.SchemaField("updated_by", "STRING"),
        bigquery.SchemaField("created_at", "INTEGER"),
        bigquery.SchemaField("updated_at", "INTEGER"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("text", "STRING"),
        bigquery.SchemaField("group_id", "STRING"),
        # New fields for Calls & Chats
        bigquery.SchemaField("call_link", "STRING"),
        bigquery.SchemaField("call_duration", "INTEGER"),
        bigquery.SchemaField("call_status", "STRING"),
        bigquery.SchemaField("call_phone", "STRING"),
        bigquery.SchemaField("call_source", "STRING"),
        bigquery.SchemaField("is_call", "BOOLEAN"),
        bigquery.SchemaField("is_chat", "BOOLEAN"),
    ]
    
    # Tasks Schema
    schema_tasks = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("entity_id", "STRING"),
        bigquery.SchemaField("entity_type", "STRING"),
        bigquery.SchemaField("responsible_user_id", "STRING"),
        bigquery.SchemaField("group_id", "STRING"),
        bigquery.SchemaField("duration", "INTEGER"),
        bigquery.SchemaField("is_completed", "BOOLEAN"),
        bigquery.SchemaField("task_type_id", "STRING"),
        bigquery.SchemaField("text", "STRING"),
        bigquery.SchemaField("result_text", "STRING"),
        bigquery.SchemaField("created_by", "STRING"),
        bigquery.SchemaField("updated_by", "STRING"),
        bigquery.SchemaField("created_at", "INTEGER"),
        bigquery.SchemaField("updated_at", "INTEGER"),
        bigquery.SchemaField("complete_till", "INTEGER"),
        bigquery.SchemaField("u_status", "INTEGER"),
        bigquery.SchemaField("account_id", "STRING"),
    ]

    schema_contact_links = [
        bigquery.SchemaField("lead_id", "STRING"),
        bigquery.SchemaField("contact_id", "STRING"),
        bigquery.SchemaField("is_primary", "BOOLEAN"),
        bigquery.SchemaField("link_source", "STRING"),
    ]

    schema_crm_events = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("entity_id", "STRING"),
        bigquery.SchemaField("entity_type", "STRING"),
        bigquery.SchemaField("created_at", "INTEGER"),
        bigquery.SchemaField("created_by", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("value_raw", "STRING"),
        bigquery.SchemaField("value_clean", "STRING"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("value_before_json", "STRING"),
        bigquery.SchemaField("value_after_json", "STRING"),
        bigquery.SchemaField("lead_status_before_id", "STRING"),
        bigquery.SchemaField("lead_status_after_id", "STRING"),
        bigquery.SchemaField("lead_status_before_pipeline_id", "STRING"),
        bigquery.SchemaField("lead_status_after_pipeline_id", "STRING"),
        bigquery.SchemaField("note_id", "STRING"),
        bigquery.SchemaField("tag_name", "STRING"),
        bigquery.SchemaField("message_id", "STRING"),
        bigquery.SchemaField("linked_entity_type", "STRING"),
        bigquery.SchemaField("linked_entity_id", "STRING"),
        bigquery.SchemaField("responsible_user_before_id", "STRING"),
        bigquery.SchemaField("responsible_user_after_id", "STRING"),
    ]

    bq.upload_data(TABLE_LEADS, cleaned_leads, schema_leads)
    bq.upload_data(TABLE_NOTES, cleaned_notes, schema_notes)
    bq.upload_data(TABLE_TASKS, cleaned_tasks, schema_tasks)
    bq.upload_data(TABLE_CONTACT_NOTES, cleaned_contact_notes, schema_notes)
    bq.upload_data(TABLE_CONTACT_TASKS, cleaned_contact_tasks, schema_tasks)
    bq.upload_data(TABLE_CRM_EVENTS, cleaned_events, schema_crm_events)
    bq.upload_data(TABLE_LEAD_CONTACT_LINKS, contact_link_rows, schema_contact_links)

    # 6. Final Dashboard Refresh (SQL)
    # This replaces the need for BigQuery Scheduled Queries
    SQL_DASHBOARD_REFRESH = f"""
    DELETE FROM `{BQ_PROJECT}.main.main` WHERE TRUE;
    INSERT INTO `{BQ_PROJECT}.main.main` 
    (date, account_id, campaign_id, adset_id, ad_id, account_name, campaign_name, adset_name, 
     ad_name, impressions, clicks, spend, source, lead, qualified, presentation, zadatok, 
     purchase, value, target_lead, interes, closed)
    WITH 
    user_dates AS (
      SELECT 
        orderId, ad_id, budget, pipeline, status, targetLead, utmCampaign,
        COALESCE(CAST(SAFE.REGEXP_EXTRACT(utmCampaign, r'(\\d+)$') AS STRING), '') as xid,
        MIN(DATE(createdAt)) OVER (PARTITION BY userId) as cohort_date
      FROM `{BQ_PROJECT}.deals_bali.deals_bali`
      WHERE pipeline IN ('Лиды Бали', 'Leads Europe', 'Leads Thailand', 'Вебинары', 'Лиды Таиланд', 'Сделки Бали', 'Europe Deals', 'Thailand Deals', 'Сделки Таиланд')
    ),
    agg_deals AS (
      SELECT
        cohort_date as dt, CAST(ad_id AS STRING) as aid, utmCampaign as utm_c, xid,
        COUNT(CASE WHEN pipeline LIKE 'Лиды%' OR pipeline LIKE 'Leads%' OR pipeline = 'Вебинары' THEN orderId END) as lead,
        COUNT(CASE WHEN (pipeline LIKE 'Лиды%' OR pipeline LIKE 'Leads%' OR pipeline = 'Вебинары') AND status IN ('Успешно реализовано', 'Closed - won') THEN orderId END) as qual,
        COUNT(CASE WHEN (pipeline LIKE '%Deals' OR pipeline LIKE 'Сделки%') AND status IN ('presentation confirmed', 'Презентация проведена') THEN orderId END) as pres,
        COUNT(CASE WHEN (pipeline LIKE '%Deals' OR pipeline LIKE 'Сделки%') AND status IN ('deposit received', 'Задаток получен') THEN orderId END) as zad,
        COUNT(CASE WHEN (pipeline LIKE '%Deals' OR pipeline LIKE 'Сделки%') AND status IN ('Успешно реализовано', 'Closed - won') THEN orderId END) as pur,
        SUM(CASE WHEN (pipeline LIKE '%Deals' OR pipeline LIKE 'Сделки%') AND status IN ('Успешно реализовано', 'Closed - won') THEN budget END) as val,
        COUNT(CASE WHEN LOWER(targetLead) LIKE 'да%' THEN orderId END) as t_lead,
        COUNT(CASE WHEN status IN ('Интерес сотрудничать подтвержден', 'intetrest in cooperating confirmed') THEN orderId END) as int_c,
        COUNT(CASE WHEN status IN ('Закрыто и не реализовано', 'Closed - lost') THEN orderId END) as cls
      FROM user_dates
      GROUP BY 1, 2, 3, 4
    ),
    ranked_ads AS (
      SELECT *,
        ROW_NUMBER() OVER (PARTITION BY date, adset_id ORDER BY spend DESC) as rn,
        ROW_NUMBER() OVER (PARTITION BY date, campaign_name ORDER BY spend DESC) as c_rn
      FROM `{BQ_PROJECT}.ready_ads.ads_data`
      WHERE impressions > 0
    )
    SELECT
      a.date, CAST(a.account_id AS STRING), CAST(a.campaign_id AS STRING), CAST(a.adset_id AS STRING), CAST(a.ad_id AS STRING),
      a.account_name, a.campaign_name, a.adset_name, a.ad_name,
      ANY_VALUE(a.impressions), ANY_VALUE(a.clicks), ANY_VALUE(a.spend), a.source,
      IFNULL(SUM(CASE WHEN a.source != 'tt' OR a.rn = 1 THEN d.lead ELSE 0 END), 0),
      IFNULL(SUM(CASE WHEN a.source != 'tt' OR a.rn = 1 THEN d.qual ELSE 0 END), 0),
      IFNULL(SUM(CASE WHEN a.source != 'tt' OR a.rn = 1 THEN d.pres ELSE 0 END), 0),
      IFNULL(SUM(CASE WHEN a.source != 'tt' OR a.rn = 1 THEN d.zad ELSE 0 END), 0),
      IFNULL(SUM(CASE WHEN a.source != 'tt' OR a.rn = 1 THEN d.pur ELSE 0 END), 0),
      IFNULL(SUM(CASE WHEN a.source != 'tt' OR a.rn = 1 THEN d.val ELSE 0 END), 0),
      IFNULL(SUM(CASE WHEN a.source != 'tt' OR a.rn = 1 THEN d.t_lead ELSE 0 END), 0),
      IFNULL(SUM(CASE WHEN a.source != 'tt' OR a.rn = 1 THEN d.int_c ELSE 0 END), 0),
      IFNULL(SUM(CASE WHEN a.source != 'tt' OR a.rn = 1 THEN d.cls ELSE 0 END), 0)
    FROM ranked_ads a
    LEFT JOIN agg_deals d ON d.dt = DATE(a.date) AND (
      -- 1. TikTok prefix match (Primary for TT)
      (a.source = 'tt' AND SUBSTR(CAST(a.adset_id AS STRING), 1, 12) = SUBSTR(d.aid, 1, 12) AND a.rn = 1) OR
      
      -- 2. Exact ad_id match (Standard for all platforms)
      (CAST(a.ad_id AS STRING) = d.aid AND d.aid NOT IN ('0', 'None', '', 'null') AND d.aid IS NOT NULL) OR
      
      -- 3. Campaign name fallback (For FB/IG/TT without valid ad_id)
      (
        (
          a.campaign_name = d.utm_c 
          OR 
          (a.source = 'tt' AND REPLACE(REPLACE(a.campaign_name, '_tt_', ''), '_tt', '') = REPLACE(REPLACE(d.utm_c, '_tt_', ''), '_tt', ''))
        ) AND a.c_rn = 1 
        AND (d.aid IS NULL OR d.aid IN ('0', 'None', '', 'null') OR LENGTH(d.aid) < 10) 
        AND IFNULL(d.utm_c, '') != ''
        AND a.source != 'ga'
      ) OR 
      
      -- 4. Google Ads campaign_id fallback 
      (a.source = 'ga' AND CAST(a.campaign_id AS STRING) = d.xid AND a.c_rn = 1 AND IFNULL(d.xid, '') != '')
    )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, a.rn, a.c_rn;
    """
    bq.execute_query(SQL_DASHBOARD_REFRESH)

    SQL_AUDIT_LAYER_REFRESH = f"""
    CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.lead_activity_enriched` AS
    WITH lead_note_activity AS (
      SELECT
        CAST(entity_id AS STRING) AS lead_id,
        'lead' AS source_entity_type,
        CAST(entity_id AS STRING) AS source_entity_id,
        id AS activity_id,
        'note' AS activity_source,
        note_type AS activity_type,
        CASE
          WHEN note_type IN ('call_in', 'call_out') THEN 'call'
          WHEN note_type IN ('message_in', 'message_out', 'talk_incoming', 'talk_outgoing', 'amomail_message') THEN 'message'
          WHEN note_type = 'attachment' THEN 'attachment'
          ELSE 'comment'
        END AS activity_category,
        created_at,
        updated_at,
        responsible_user_id,
        text,
        is_call,
        is_chat,
        call_duration,
        call_status,
        CAST(NULL AS BOOL) AS is_completed,
        CAST(NULL AS INT64) AS complete_till
      FROM `{BQ_PROJECT}.{BQ_DATASET}.notes`
    ),
    lead_task_activity AS (
      SELECT
        CAST(entity_id AS STRING) AS lead_id,
        'lead' AS source_entity_type,
        CAST(entity_id AS STRING) AS source_entity_id,
        id AS activity_id,
        'task' AS activity_source,
        task_type_id AS activity_type,
        'task' AS activity_category,
        created_at,
        updated_at,
        responsible_user_id,
        COALESCE(NULLIF(result_text, ''), text) AS text,
        FALSE AS is_call,
        FALSE AS is_chat,
        CAST(NULL AS INT64) AS call_duration,
        CAST(NULL AS STRING) AS call_status,
        is_completed,
        complete_till
      FROM `{BQ_PROJECT}.{BQ_DATASET}.tasks`
    ),
    contact_note_activity AS (
      SELECT
        lcl.lead_id,
        'contact' AS source_entity_type,
        CAST(cn.entity_id AS STRING) AS source_entity_id,
        cn.id AS activity_id,
        'note' AS activity_source,
        cn.note_type AS activity_type,
        CASE
          WHEN cn.note_type IN ('call_in', 'call_out') THEN 'call'
          WHEN cn.note_type IN ('message_in', 'message_out', 'talk_incoming', 'talk_outgoing', 'amomail_message') THEN 'message'
          WHEN cn.note_type = 'attachment' THEN 'attachment'
          ELSE 'comment'
        END AS activity_category,
        cn.created_at,
        cn.updated_at,
        cn.responsible_user_id,
        cn.text,
        cn.is_call,
        cn.is_chat,
        cn.call_duration,
        cn.call_status,
        CAST(NULL AS BOOL) AS is_completed,
        CAST(NULL AS INT64) AS complete_till
      FROM `{BQ_PROJECT}.{BQ_DATASET}.contact_notes` cn
      JOIN `{BQ_PROJECT}.{BQ_DATASET}.lead_contact_links` lcl
        ON CAST(cn.entity_id AS STRING) = lcl.contact_id
    ),
    contact_task_activity AS (
      SELECT
        lcl.lead_id,
        'contact' AS source_entity_type,
        CAST(ct.entity_id AS STRING) AS source_entity_id,
        ct.id AS activity_id,
        'task' AS activity_source,
        ct.task_type_id AS activity_type,
        'task' AS activity_category,
        ct.created_at,
        ct.updated_at,
        ct.responsible_user_id,
        COALESCE(NULLIF(ct.result_text, ''), ct.text) AS text,
        FALSE AS is_call,
        FALSE AS is_chat,
        CAST(NULL AS INT64) AS call_duration,
        CAST(NULL AS STRING) AS call_status,
        ct.is_completed,
        ct.complete_till
      FROM `{BQ_PROJECT}.{BQ_DATASET}.contact_tasks` ct
      JOIN `{BQ_PROJECT}.{BQ_DATASET}.lead_contact_links` lcl
        ON CAST(ct.entity_id AS STRING) = lcl.contact_id
    )
    SELECT * FROM lead_note_activity
    UNION ALL
    SELECT * FROM lead_task_activity
    UNION ALL
    SELECT * FROM contact_note_activity
    UNION ALL
    SELECT * FROM contact_task_activity;

    CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.lead_stage_history` AS
    SELECT
      CAST(entity_id AS STRING) AS lead_id,
      entity_type,
      type,
      created_at,
      lead_status_before_id,
      lead_status_after_id,
      lead_status_before_pipeline_id,
      lead_status_after_pipeline_id,
      note_id,
      tag_name,
      linked_entity_type,
      linked_entity_id,
      responsible_user_before_id,
      responsible_user_after_id
    FROM `{BQ_PROJECT}.{BQ_DATASET}.crm_events`
    WHERE entity_type = 'lead';

    CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.lead_defer_signals` AS
    SELECT
      lead_id,
      source_entity_type,
      source_entity_id,
      activity_id,
      created_at,
      responsible_user_id,
      text
    FROM `{BQ_PROJECT}.{BQ_DATASET}.lead_activity_enriched`
    WHERE activity_category = 'comment'
      AND REGEXP_CONTAINS(
        LOWER(IFNULL(text, '')),
        r'(через|позже|пізніше|пізн|написат|написати|набрать|набрати|недел|тиж|мес|місяц|месяц|позвонить позже|попросил позже|попросив пізніше)'
      );
    """
    bq.execute_query(SQL_AUDIT_LAYER_REFRESH)

    logger.info("Export and Dashboard Refresh completed successfully.")

if __name__ == "__main__":
    # If run directly (local), just run the logic.
    # If PORT is set, assumes standard Cloud Run container behavior.
    port = os.environ.get("PORT")
    if port:
        app.run(host="0.0.0.0", port=int(port))
    else:
        main_logic()
