
# etl_service.py
import os
import sys
import time
import json
import logging
import signal
import requests
import mysql.connector
import phonenumbers
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

# --- Configuration ---

class Config:
    def __init__(self):
        # Find .env in current directory
        env_path = Path(__file__).parent / '.env'
        if env_path.exists():
            load_dotenv(env_path)
            logging.info(f"Loaded config from {env_path}")
        else:
            logging.warning(f".env not found at {env_path}, relying on system env vars")

        # Core Settings
        self.customer = os.getenv('ETL_CLIENT_NAME', 'spc').lower() # Default to SPC if not set
        self.interval_minutes = int(os.getenv('FETCH_INTERVAL_MINUTES', 5))
        
        # Database
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.db_user = os.getenv('DB_USER', 'root')
        self.db_password = os.getenv('DB_PASSWORD')
        self.db_port = int(os.getenv('DB_PORT', 3306))
        
        # Customer Specific
        prefix = self.customer.upper()
        self.api_base_url = os.getenv(f'{prefix}_BASE_URL')
        # Handle difference in naming (API_BASE_URL vs BASE_URL in .env)
        if not self.api_base_url: 
             self.api_base_url = os.getenv(f'{prefix}_API_BASE_URL')
             
        self.api_username = os.getenv(f'{prefix}_API_USERNAME')
        self.api_password = os.getenv(f'{prefix}_API_PASSWORD')
        self.api_account_id = os.getenv(f'{prefix}_API_ACCOUNT_ID')
        self.api_tenant = os.getenv(f'{prefix}_API_TENANT')
        self.db_name = os.getenv(f'{prefix}_DB_NAME', f'allcdr_{self.customer}')
        
        # Subdisposition retry settings
        self.subdisposition_retry_min_age = int(os.getenv('SUBDISPOSITION_RETRY_MIN_AGE_MINUTES', 5))
        self.subdisposition_retry_max_age = int(os.getenv('SUBDISPOSITION_RETRY_MAX_AGE_MINUTES', 70))
        
        if not self.api_base_url or not self.db_name:
            raise ValueError(f"Missing configuration for customer '{prefix}'. Check .env")

class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            if self.conn and self.conn.is_connected():
                return
            
            # First connect without database to ensure server is reachable and create DB if needed
            initial_conn = mysql.connector.connect(
                host=self.config.db_host,
                user=self.config.db_user,
                password=self.config.db_password,
                port=self.config.db_port
            )
            initial_cursor = initial_conn.cursor()
            
            # Check if database exists
            initial_cursor.execute(f"SHOW DATABASES LIKE '{self.config.db_name}'")
            exists = initial_cursor.fetchone()
            
            if not exists:
                logging.warning(f"Database '{self.config.db_name}' does not exist. Creating and initializing...")
                initial_cursor.execute(f"CREATE DATABASE {self.config.db_name}")
                logging.info(f"Database '{self.config.db_name}' created.")
                
                # Connect to the new database to initialize schema
                self.conn = mysql.connector.connect(
                    host=self.config.db_host,
                    user=self.config.db_user,
                    password=self.config.db_password,
                    port=self.config.db_port,
                    database=self.config.db_name
                )
                self.cursor = self.conn.cursor()
                self.initialize_schema()
            else:
                initial_cursor.close()
                initial_conn.close()
                
                # Normal connection
                self.conn = mysql.connector.connect(
                    host=self.config.db_host,
                    user=self.config.db_user,
                    password=self.config.db_password,
                    port=self.config.db_port,
                    database=self.config.db_name
                )
                self.cursor = self.conn.cursor()
                
            logging.info(f"Connected to database: {self.config.db_name}")
            
        except mysql.connector.Error as e:
            logging.error(f"Database connection failed: {e}")
            raise

    def initialize_schema(self):
        """Initializes the database schema from the unified SQL file."""
        import re
        try:
            # Locate the schema file - check Docker path first, then relative path
            docker_schema_path = Path('/app/schema/phase3_complete_unified.sql')
            relative_schema_path = Path(__file__).parent.parent / '03_database_schemas' / 'phase3_complete_unified.sql'
            
            if docker_schema_path.exists():
                schema_path = docker_schema_path
            elif relative_schema_path.exists():
                schema_path = relative_schema_path
            else:
                logging.error(f"Schema file not found at: {docker_schema_path} or {relative_schema_path}")
                raise FileNotFoundError(f"Schema file missing")
                
            logging.info(f"Applying schema from: {schema_path}")
            
            with open(schema_path, 'r') as f:
                sql_content = f.read()
            
            # Split SQL file into separate statements
            # Note: This is a basic parser. Complex delimiters in the SQL file might require robust parsing.
            # The current SQL file uses DELIMITER // logic for procedures, which needs special handling.
            
            # Robust logic: We need to handle the DELIMITER commands manually or use a connector feature
            # Standard connector doesn't handle 'DELIMITER' keyword well in execute().
            # We will strip comments and try to execute statement by statement.
            
            # Simple approach: Removing comments and empty lines
            statements = []
            delimiter = ';'
            current_stmt = []
            
            lines = sql_content.split('\n')
            for line in lines:
                line_stripped = line.strip()
                if not line_stripped or line_stripped.startswith('--'):
                    continue
                    
                if line_stripped.upper().startswith('DELIMITER'):
                    delimiter = line_stripped.split()[1]
                    continue
                
                current_stmt.append(line)
                
                if line_stripped.endswith(delimiter):
                    # Remove the delimiter from the end
                    stmt_str = '\n'.join(current_stmt)
                    # Handle custom delimiter stripping properly
                    if delimiter != ';':
                         stmt_str = stmt_str.rsplit(delimiter, 1)[0]
                    else:
                         stmt_str = stmt_str[:-1]

                    statements.append(stmt_str)
                    current_stmt = []
                    
            # Execute each statement
            for stmt in statements:
                if not stmt.strip(): continue
                try:
                    self.cursor.execute(stmt)
                except mysql.connector.Error as err:
                    # Ignore harmless errors like "table already exists" if using IF NOT EXISTS
                    # But procedures might throw errors if they exist.
                     logging.warning(f"Schema warning (ignoring): {err.msg}")
            
            self.conn.commit()
            logging.info("Schema initialized successfully.")
            
        except Exception as e:
            logging.error(f"Failed to initialize schema: {e}")
            raise
            
    def close(self):
        if self.conn and self.conn.is_connected():
            self.cursor.close()
            self.conn.close()
            logging.info("Database connection closed")

    def maintain_connection(self):
        """Reconnect if connection lost"""
        try:
            if self.conn:
                self.conn.ping(reconnect=True, attempts=3, delay=2)
                self.cursor = self.conn.cursor() # Re-get cursor
        except mysql.connector.Error:
            logging.warning("Connection lost, reconnecting...")
            self.connect()

class AuthManager:
    def __init__(self, config):
        self.config = config
        self.token = None
        self.token_expiry = 0

    def get_token(self):
        # If token exists and valid (buffer 60s), return it.
        if self.token and time.time() < self.token_expiry - 60:
            return self.token

        logging.info("Acquiring new API token...")
        
        base = self.config.api_base_url.rstrip('/')
        # Candidate endpoints from run_pipeline.py
        candidates = [
            {"url": f"{base}/api/v2/config/login/oauth", "body": {"domain": self.config.api_tenant, "username": self.config.api_username, "password": self.config.api_password}},
            {"url": f"{base}/api/v2/login", "body": {"domain": self.config.api_tenant, "username": self.config.api_username, "password": self.config.api_password}},
            {"url": f"{base}/api/login", "body": {"domain": self.config.api_tenant, "username": self.config.api_username, "password": self.config.api_password}},
        ]
        
        last_error = None
        
        for candidate in candidates:
            url = candidate["url"]
            body = candidate["body"]
            
            # Simple retry per endpoint
            for attempt in range(3):
                try:
                    resp = requests.post(url, json=body, verify=False, timeout=10)
                    resp.raise_for_status()
                    data = resp.json()
                    
                    access = data.get("accessToken") or data.get("access_token")
                    if access:
                        self.token = access
                        self.token_expiry = time.time() + 3600 # Assume 1h validity
                        logging.info(f"Token acquired successfully via {url}")
                        return self.token
                    else:
                        logging.warning(f"No access token in response from {url}")
                        
                except Exception as e:
                    last_error = e
                    if attempt < 2:
                        time.sleep(1)
                    else:
                        logging.debug(f"Failed to auth against {url}: {e}")
        
        logging.error("All auth candidates failed.")
        raise last_error if last_error else Exception("Authentication failed")

# --- Worker ---

class ETLWorker:
    def __init__(self):
        self.config = Config()
        self.db = DatabaseManager(self.config)
        self.auth = AuthManager(self.config)
        self.dim_cache = {
             "users": {}, "call_disposition": {}, "system": {}, "campaigns": {}, "queues": {},
             "date": {}, "time_of_day": {}
        }
        # Disable SSL warnings
        requests.packages.urllib3.disable_warnings()

    def run_ingestion(self, start_unix=None, end_unix=None):
        """Phase 1: Fetch and Insert"""
        
        if start_unix is None or end_unix is None:
            fetch_minutes = self.config.interval_minutes
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(minutes=fetch_minutes)
            
            start_unix = int(start_time.timestamp())
            end_unix = int(end_time.timestamp())
            
        logging.info(f"--- Starting Ingestion for {self.config.customer.upper()} ---")
        logging.info(f"Time range: {datetime.fromtimestamp(start_unix, tz=timezone.utc)} to {datetime.fromtimestamp(end_unix, tz=timezone.utc)}")
        
        token = self.auth.get_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'x-account-id': self.config.api_account_id
        }
        
        records_ingested = 0
        
        # Pagination loop
        start_key = None
        while True:
            params = {
                'startDate': start_unix,
                'endDate': end_unix,
                'pageSize': 100000
            }
            if start_key:
                params['start_key'] = start_key
                
            url = f"{self.config.api_base_url}/api/v2/reports/cdrs/all"
            
            try:
                resp = requests.get(url, headers=headers, params=params, verify=False, timeout=120)
                
                # Check for non-200 status before trying to parse JSON
                if resp.status_code != 200:
                    logging.error(f"API Request Failed. Status: {resp.status_code}, Body: {resp.text[:500]}")
                    resp.raise_for_status()
                
                if not resp.text.strip():
                    logging.warning(f"Empty response body received from {url}. Assuming no records.")
                    data = {}
                else:
                    try:
                        data = resp.json()
                    except ValueError as e:
                        logging.error(f"Invalid JSON response. Status: {resp.status_code}, URL: {url}")
                        logging.error(f"Response body preview: {resp.text[:1000]}")
                        raise e
                
                records = data.get('cdrs', [])
                if not records:
                    break
                    
                # Insert
                self.db.maintain_connection()
                insert_query = "INSERT IGNORE INTO cdr_raw_data (msg_id, record_data) VALUES (%s, %s)"
                vals = []
                for r in records:
                    if r.get('msg_id'):
                        vals.append((r['msg_id'], json.dumps(r)))
                
                if vals:
                    self.db.cursor.executemany(insert_query, vals)
                    self.db.conn.commit()
                    records_ingested += self.db.cursor.rowcount
                
                start_key = data.get('new_start_key')
                if not start_key:
                    break
                    
            except Exception as e:
                logging.error(f"Ingestion error: {e}")
                time.sleep(5) # Short wait on error
                break
                
        logging.info(f"Ingestion complete. New records: {records_ingested}")

    def run_transformation(self):
        """Phase 2: Transform and Load"""
        logging.info("--- Starting Transformation ---")
        
        self.db.maintain_connection()
        
        # Process in batches
        while True:
            self.db.cursor.execute("SELECT id, record_data FROM cdr_raw_data WHERE etl_processed_at IS NULL LIMIT 500")
            records = self.db.cursor.fetchall()
            
            if not records:
                break
            
            logging.info(f"Processing batch of {len(records)} records...")
            
            # Transaction Retry Loop
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    processed_cnt = 0
                    for row_id, raw_json in records:
                        try:
                            cdr = json.loads(raw_json)
                            self._process_single_cdr(cdr)
                            
                            # Mark processed
                            self.db.cursor.execute("UPDATE cdr_raw_data SET etl_processed_at = NOW() WHERE id = %s", (row_id,))
                            processed_cnt += 1
                        except Exception as e:
                            logging.error(f"Failed record {row_id}: {e}")
                            # Mark failed preventing eternal retry loop
                            self.db.cursor.execute("UPDATE cdr_raw_data SET etl_processed_at = '1970-01-01 00:00:00' WHERE id = %s", (row_id,))
                    
                    self.db.conn.commit()
                    break # Success, exit retry loop
                    
                except mysql.connector.Error as e:
                    if e.errno in [1205, 1213]: # Lock wait timeout or Deadlock
                        logging.warning(f"Transaction deadlock/timeout (attempt {attempt+1}/{max_retries}): {e}")
                        self.db.conn.rollback()
                        time.sleep(2 * (attempt + 1)) # Backoff
                        if attempt == max_retries - 1:
                            logging.error("Max retries reached for batch transaction.")
                            raise e
                    else:
                        raise e
            
        logging.info("Transformation complete.")

    def _process_single_cdr(self, cdr):
        # Timestamp conversion
        ts_val = cdr.get('timestamp')
        ts = self._convert_timestamp(ts_val)
        
        date_key = int(ts.strftime('%Y%m%d'))
        time_key = int(ts.strftime('%H%M%S'))
        
        # Dimensions
        self._ensure_time_dimensions(ts, date_key, time_key)
        
        caller_key = self._get_user_key(cdr.get('caller_id_number'), cdr.get('caller_id_name'))
        callee_key = self._get_user_key(cdr.get('callee_id_number'), cdr.get('callee_id_name'))
        
        # Disposition
        fono = cdr.get('fonoUC', {})
        sub1 = fono.get('subdisposition', {})
        if isinstance(sub1, dict): sub1_name = sub1.get('name')
        else: sub1_name = str(sub1) if sub1 else None
        
        # Store raw subdisposition for later processing/retry
        subdisposition_raw = None
        if sub1:
            subdisposition_raw = json.dumps(sub1) if isinstance(sub1, dict) else str(sub1)
        
        disp_key = self._get_dim_key("call_disposition", {
            "call_direction": cdr.get('call_direction'),
            "hangup_cause": cdr.get('hangup_cause'),
            "disposition": fono.get('disposition') or cdr.get('disposition'),
            "subdisposition_1": sub1_name
        })
        
        # Fact
        fact = {
            "msg_id": cdr.get('msg_id'),
            "call_id": cdr.get('call_id'),
            "date_key": date_key,
            "time_key": time_key,
            "caller_user_key": caller_key,
            "callee_user_key": callee_key,
            "disposition_key": disp_key,
            "duration_seconds": cdr.get('duration_seconds', 0),
            "billing_seconds": cdr.get('billing_seconds', 0),
            "is_conference": 1 if cdr.get('is_conference') else 0,
            "subdisposition_raw": subdisposition_raw
        }
        
        cols = ", ".join(fact.keys())
        placeholders = ", ".join(["%s"] * len(fact))
        sql = f"INSERT IGNORE INTO fact_calls ({cols}) VALUES ({placeholders})"
        self.db.cursor.execute(sql, list(fact.values()))

    # --- Helpers ---
    
    def _convert_timestamp(self, ts):
        """
        Convert various timestamp formats to datetime.
        Handles:
        - Gregorian seconds (seconds since year 0) - used by Kazoo/Erlang
        - Unix seconds (seconds since 1970)
        - Unix milliseconds
        - Unix microseconds
        """
        if not ts: return datetime.utcnow()
        
        # Gregorian to Unix epoch offset (seconds from year 0 to 1970-01-01)
        GREGORIAN_EPOCH_OFFSET = 62167219200
        
        try:
            val = float(ts)
            
            # Detect Gregorian seconds (Kazoo/Erlang format)
            # Gregorian timestamps for dates 2000-2100 are roughly 63-66 billion
            if 62000000000 < val < 70000000000:
                # Convert Gregorian seconds to Unix seconds
                unix_ts = val - GREGORIAN_EPOCH_OFFSET
                return datetime.fromtimestamp(unix_ts)
            
            # Handle other formats based on magnitude
            if val > 1e15:  # Microseconds
                return datetime.fromtimestamp(val / 1e6)
            elif val > 1e12:  # Milliseconds
                return datetime.fromtimestamp(val / 1e3)
            else:  # Unix seconds
                return datetime.fromtimestamp(val)
                
        except Exception as e:
            logging.warning(f"Timestamp conversion failed for {ts}: {e}")
            return datetime.utcnow()

    def _ensure_time_dimensions(self, ts, d_key, t_key):
        # Date
        if d_key not in self.dim_cache['date']:
            sql = "INSERT IGNORE INTO dim_date (date_key, full_date, year, quarter, month, day_of_week) VALUES (%s, %s, %s, %s, %s, %s)"
            self.db.cursor.execute(sql, (d_key, ts.date(), ts.year, (ts.month-1)//3+1, ts.month, ts.strftime('%A')))
            self.dim_cache['date'][d_key] = True
            
        # Time
        if t_key not in self.dim_cache['time_of_day']:
            sql = "INSERT IGNORE INTO dim_time_of_day (time_key, full_time, hour, minute) VALUES (%s, %s, %s, %s)"
            self.db.cursor.execute(sql, (t_key, ts.time(), ts.hour, ts.minute))
            self.dim_cache['time_of_day'][t_key] = True

    def _get_user_key(self, number, name):
        if not number: return None
        # Basic cache check
        if number in self.dim_cache['users']: return self.dim_cache['users'][number]
        
        try:
            # Insert
            sql = "INSERT INTO dim_users (user_number, user_name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE user_name = VALUES(user_name)"
            self.db.cursor.execute(sql, (number, name))
            
            # Retrieve ID
            self.db.cursor.execute("SELECT user_key FROM dim_users WHERE user_number = %s", (number,))
            res = self.db.cursor.fetchone()
            if res:
                key = res[0]
                self.dim_cache['users'][number] = key
                return key
        except mysql.connector.Error as e:
            logging.warning(f"Failed to process user {number}: {e}")
            return None
        return None

    def _get_dim_key(self, table, criteria):
        # Simplified dimension lookup
        # In a full impl regarding performance, we'd hash criteria.
        # Here we just insert ignore and select.
        keys = list(criteria.keys())
        vals = list(criteria.values())
        where = " AND ".join([f"{k} = %s" if v is not None else f"{k} IS NULL" for k,v in criteria.items()])
        non_null_vals = [v for v in vals if v is not None]
        
        # Select
        pk = "disposition_key" if table == "call_disposition" else f"{table}_key"
        self.db.cursor.execute(f"SELECT {pk} FROM dim_{table} WHERE {where}", non_null_vals)
        res = self.db.cursor.fetchone()
        if res: return res[0]
        
        # Insert
        cols = ", ".join(keys)
        placeholders = ", ".join(["%s"] * len(keys))
        sql = f"INSERT INTO dim_{table} ({cols}) VALUES ({placeholders})"
        self.db.cursor.execute(sql, vals)
        return self.db.cursor.lastrowid

    def run_subdisposition_retry(self):
        """
        Fallback retry mechanism for subdisposition data.
        Detects calls where subdisposition_raw is still NULL and attempts to update
        from the raw CDR data if subdisposition has become available.
        
        Retry window: Between min_age and max_age minutes (default 5-70 minutes)
        """
        min_age = self.config.subdisposition_retry_min_age
        max_age = self.config.subdisposition_retry_max_age
        
        logging.info(f"--- Starting Subdisposition Retry (window: {min_age}-{max_age} min) ---")
        
        self.db.maintain_connection()
        
        # Find fact_calls records with NULL subdisposition_raw within retry window
        query = """
            SELECT fc.call_key, fc.msg_id, cdr.record_data
            FROM fact_calls fc
            JOIN cdr_raw_data cdr ON fc.msg_id = cdr.msg_id
            WHERE fc.subdisposition_raw IS NULL
              AND fc.created_at BETWEEN NOW() - INTERVAL %s MINUTE 
                                    AND NOW() - INTERVAL %s MINUTE
            LIMIT 500
        """
        
        try:
            self.db.cursor.execute(query, (max_age, min_age))
            records = self.db.cursor.fetchall()
            
            if not records:
                logging.info("No records require subdisposition retry.")
                return
            
            logging.info(f"Found {len(records)} records for subdisposition retry.")
            
            updated_count = 0
            for call_key, msg_id, raw_json in records:
                try:
                    cdr = json.loads(raw_json)
                    fono = cdr.get('fonoUC', {})
                    sub1 = fono.get('subdisposition', {})
                    
                    # Check if subdisposition is now available
                    if sub1:
                        subdisposition_raw = json.dumps(sub1) if isinstance(sub1, dict) else str(sub1)
                        
                        # Extract subdisposition name for dimension update
                        if isinstance(sub1, dict):
                            sub1_name = sub1.get('name')
                        else:
                            sub1_name = str(sub1) if sub1 else None
                        
                        # Get or create updated disposition key
                        disp_key = self._get_dim_key("call_disposition", {
                            "call_direction": cdr.get('call_direction'),
                            "hangup_cause": cdr.get('hangup_cause'),
                            "disposition": fono.get('disposition') or cdr.get('disposition'),
                            "subdisposition_1": sub1_name
                        })
                        
                        # Update fact_calls with subdisposition data
                        # Double-check subdisposition_raw IS NULL to prevent race conditions
                        update_sql = """
                            UPDATE fact_calls 
                            SET subdisposition_raw = %s, disposition_key = %s
                            WHERE call_key = %s 
                              AND subdisposition_raw IS NULL
                        """
                        self.db.cursor.execute(update_sql, (subdisposition_raw, disp_key, call_key))
                        
                        if self.db.cursor.rowcount > 0:
                            updated_count += 1
                            logging.debug(f"Updated subdisposition for call_key {call_key}")
                            
                except Exception as e:
                    logging.warning(f"Failed to retry subdisposition for call_key {call_key}: {e}")
            
            self.db.conn.commit()
            logging.info(f"Subdisposition retry complete. Updated {updated_count}/{len(records)} records.")
            
        except Exception as e:
            logging.error(f"Subdisposition retry failed: {e}")

    def loop(self):
        self.db.connect()
        logging.info(f"Service started. Interval: {self.config.interval_minutes}m")
        logging.info(f"Subdisposition retry window: {self.config.subdisposition_retry_min_age}-{self.config.subdisposition_retry_max_age} min")
        
        while True:
            # Check for pause flag set by manual job
            while os.path.exists('/tmp/etl_paused.flag'):
                logging.info("Main loop paused: Manual job running. Waiting 30s...")
                time.sleep(30)
                
            try:
                start_cycle = time.time()
                self.run_ingestion()
                self.run_transformation()
                self.run_subdisposition_retry()
                
                # Sleep rest of interval
                elapsed = time.time() - start_cycle
                sleep_sec = (self.config.interval_minutes * 60) - elapsed
                if sleep_sec > 0:
                    logging.info(f"Cycle finished. Sleeping {sleep_sec:.0f}s...")
                    time.sleep(sleep_sec)
                else:
                    logging.warning("Cycle took longer than interval, starting immediately.")
                    
            except KeyboardInterrupt:
                logging.info("Stopping service...")
                break
            except Exception as e:
                logging.critical(f"Critical error in loop: {e}")
                time.sleep(60) # Wait before retry

if __name__ == "__main__":
    # Setup Log
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler("etl_service.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    worker = ETLWorker()

    import argparse
    parser = argparse.ArgumentParser(description='Standalone ETL Service')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()

    if args.start_date and args.end_date:
        try:
            # Parse dates (assume UTC)
            start_dt = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_dt = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            # Set end date to end of day (23:59:59)
            end_dt = end_dt.replace(hour=23, minute=59, second=59)
            
            start_unix = int(start_dt.timestamp())
            end_unix = int(end_dt.timestamp())
            
            # Create pause flag
            pause_flag = '/tmp/etl_paused.flag'
            with open(pause_flag, 'w') as f:
                f.write(str(os.getpid()))
            
            try:
                logging.info(f"Running one-off ETL for range: {start_dt} to {end_dt}")
                
                worker.db.connect()
                worker.run_ingestion(start_unix=start_unix, end_unix=end_unix)
                worker.run_transformation()
                logging.info("One-off ETL run complete.")
            finally:
                if os.path.exists(pause_flag):
                    os.remove(pause_flag)
            
        except ValueError as e:
            logging.error(f"Invalid date format. Please use YYYY-MM-DD. Error: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"ETL run failed: {e}")
            sys.exit(1)
    else:
        worker.loop()