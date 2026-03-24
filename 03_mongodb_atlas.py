# NorthStar Urban Mobility – MongoDB Atlas Development
# Databases and Analytics Assignment
# PyMongo: NoSQL Design, CRUD, Aggregation & Optimisation

# Install: pip install pymongo[srv] pandas
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from pymongo.errors import BulkWriteError
import pandas as pd
import numpy as np
import json
from datetime import datetime
import pprint

# 1. CONNECTION
# Replace with the MongoDB Atlas connection string
CONNECTION_STRING = "mongodb+srv://<username>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(CONNECTION_STRING)
db = client["northstar_db"]
print("Connected to MongoDB Atlas:", db.name)

# 2. DATA LOADING & CLEANING
def standardise_zone(z):
    if pd.isna(z): return None
    z = str(z).strip()
    mapping = {
        'airport':'Airport', 'AIRPORT':'Airport',
        'central':'Central', 'CENTRAL':'Central', 'Ctr':'Central',
        'east':'East', 'EAST':'East',
        'north':'North', 'NORTH':'North',
        'south':'South', 'SOUTH':'South',
        'west':'West', 'WEST':'West',
        'riverside':'Riverside', 'RiverSide':'Riverside'
    }
    return mapping.get(z, z.title())

dfs = {}
for f in ['customers','orders','deliveries','drivers','vehicles','hubs','complaints','incidents','app_events']:
    dfs[f] = pd.read_csv(f'{f}.csv')

for df_name in ['customers','orders','drivers','vehicles']:
    for col in dfs[df_name].columns:
        if 'zone' in col.lower():
        dfs[df_name][col] = dfs[df_name][col].apply(standardise_zone)

dfs['app_events']['zone_context'] = dfs['app_events']['zone_context'].apply(standardise_zone)

# 3. NOSQL DOCUMENT DESIGN RATIONALE
"""
DESIGN DECISION:
NorthStar requires three MongoDB collections reflecting operational reality:
 
  Collection 1: customer_cases
    - Embeds complaint history, order references, and app event sequences
    - Enables single-document retrieval for full customer case view
    - Supports escalation tracking without cross-collection joins

  Collection 2: delivery_operations  
    - Embeds driver, vehicle, and hub context at point of delivery
    - Embeds all incidents linked to the delivery
    - Represents event snapshot — no dependency on normalised state

  Collection 3: app_session_events
    - Models session-level event streams as arrays within session documents
    - Enables time-series queries on customer behaviour without row explosion
    
This design avoids the rigid FK constraints that prevent NorthStar from
combining exception records, nested histories, and semi-structured data.
"""

# 4. BUILD AND INSERT COLLECTION 1: customer_cases
print("\n=== Building customer_cases collection ===")

customers_df = dfs['customers'].copy()
complaints_df = dfs['complaints'].copy()
orders_df = dfs['orders'].copy()

# Build complaint sub-documents per customer
complaint_map = {}
for _, row in complaints_df.iterrows():
    cid = row['customer_id']
    complaint_doc = {
        "complaint_id": row['complaint_id'],
        "order_id": row['order_id'] if pd.notna(row['order_id']) else None,
        "complaint_type": row['complaint_type'],
        "channel": row['channel'],
        "severity": row['severity'],
        "created_at": row['created_at'],
        "status": row['status'],
        "resolution_days": int(row['resolution_days']) if pd.notna(row['resolution_days']) else None,
        "compensation_amount": float(row['compensation_amount']) if pd.notna(row['compensation_amount']) else None
    }
    complaint_map.setdefault(cid, []).append(complaint_doc)

# Build order summary per customer
order_map = {}
for _, row in orders_df.iterrows():
    cid = row['customer_id']
    order_map.setdefault(cid, []).append({
        "order_id": row['order_id'],
        "service_type": row['service_type'],
        "order_value": float(row['order_value']) if pd.notna(row['order_value']) else None,
        "priority_level": row['priority_level'],
        "pickup_zone": row['pickup_zone'],
        "dropoff_zone": row['dropoff_zone'],
        "booking_channel": row['booking_channel'] if pd.notna(row['booking_channel']) else None
    })

# Assemble customer case documents
customer_docs = []
for _, cust in customers_df.iterrows():
    cid = cust['customer_id']
    doc = {
        "customer_id": cid,
        "age": int(cust['age']) if pd.notna(cust['age']) else None,
        "home_zone": cust['home_zone'],
        "customer_type": cust['customer_type'],
        "signup_date": cust['signup_date'],
        "loyalty_score": float(cust['loyalty_score']) if pd.notna(cust['loyalty_score']) else None,
        "app_engagement_score": float(cust['app_engagement_score']) if pd.notna(cust['app_engagement_score']) else None,
        "preferred_channel": cust['preferred_channel'] if pd.notna(cust['preferred_channel']) else "Unknown",
        "account_status": cust['account_status'],
        "complaint_history": complaint_map.get(cid, []),
        "order_summary": order_map.get(cid, []),
        "total_complaints": len(complaint_map.get(cid, [])),
        "total_orders": len(order_map.get(cid, [])),
        "created_at": datetime.utcnow()
    }
    customer_docs.append(doc)

# Drop and insert
db.customer_cases.drop()
result = db.customer_cases.insert_many(customer_docs)
print(f"  Inserted {len(result.inserted_ids)} customer case documents")

# Preview
print("\nSample customer_cases document:")
pprint.pprint(db.customer_cases.find_one({"total_complaints": {"$gte": 2}}))

# 5. BUILD COLLECTION 2: delivery_operations
print("\n=== Building delivery_operations collection ===")

deliveries_df = dfs['deliveries'].copy()
drivers_df = dfs['drivers'].copy()
vehicles_df = dfs['vehicles'].copy()
incidents_df = dfs['incidents'].copy()

# Build driver lookup
driver_lookup = {r['driver_id']: r.to_dict() for _, r in drivers_df.iterrows()}
# Build vehicle lookup
vehicle_lookup = {r['vehicle_id']: r.to_dict() for _, r in vehicles_df.iterrows()}
# Build incident map
incident_map = {}
for _, row in incidents_df.iterrows():
    did = row['delivery_id']
    incident_map.setdefault(did, []).append({
        "incident_id": row['incident_id'],
        "incident_type": row['incident_type'],
        "reported_at": row['reported_at'],
        "severity": row['severity'],
        "resolution_status": row['resolution_status'],
        "resolved_hours": float(row['resolved_hours']) if pd.notna(row['resolved_hours']) else None
    })

delivery_docs = []
for _, row in deliveries_df.iterrows():
    did = row['delivery_id']
    drv = driver_lookup.get(row['driver_id'], {})
    veh = vehicle_lookup.get(row['vehicle_id'], {})
    doc = {
        "delivery_id": did,
        "order_id": row['order_id'],
        "hub_id": row['hub_id'],
        "dispatch_time": row['dispatch_time'],
        "delivery_completed_at": row['delivery_completed_at'] if pd.notna(row.get('delivery_completed_at')) else None,
        "delivery_status": row['delivery_status'],
        "route_distance_km": float(row['route_distance_km']) if pd.notna(row['route_distance_km']) else None,
        "manual_route_override_count": int(row['manual_route_override_count']),
        "proof_of_completion_missing": bool(row['proof_of_completion_missing']),
        "customer_rating_post_delivery": float(row['customer_rating_post_delivery']) if pd.notna(row['customer_rating_post_delivery']) else None,
        "fuel_or_charge_cost": float(row['fuel_or_charge_cost']) if pd.notna(row['fuel_or_charge_cost']) else None,
        "driver_snapshot": {
            "driver_id": row['driver_id'],
            "employment_type": drv.get('employment_type'),
            "base_zone": drv.get('base_zone'),
            "years_experience": int(drv['years_experience']) if drv.get('years_experience') and pd.notna(drv['years_experience']) else None,
            "training_score": float(drv['training_score']) if drv.get('training_score') and pd.notna(drv['training_score']) else None,
            "driver_rating": float(drv['driver_rating']) if drv.get('driver_rating') and pd.notna(drv['driver_rating']) else None,
        },
        "vehicle_snapshot": {
            "vehicle_id": row['vehicle_id'],
            "vehicle_type": veh.get('vehicle_type'),
            "assigned_zone": veh.get('assigned_zone'),
            "battery_health_pct": float(veh['battery_health_pct']) if veh.get('battery_health_pct') and pd.notna(veh['battery_health_pct']) else None,
            "maintenance_status": veh.get('maintenance_status'),
            "odometer_km": int(veh['odometer_km']) if veh.get('odometer_km') and pd.notna(veh['odometer_km']) else None,
        },
        "incidents": incident_map.get(did, []),
        "incident_count": len(incident_map.get(did, [])),
        "created_at": datetime.utcnow()
    }
    delivery_docs.append(doc)

db.delivery_operations.drop()
result = db.delivery_operations.insert_many(delivery_docs)
print(f"  Inserted {len(result.inserted_ids)} delivery_operations documents")

# 6. BUILD COLLECTION 3: app_session_events
print("\n=== Building app_session_events collection ===")

ae_df = dfs['app_events'].copy()
ae_df['event_timestamp'] = pd.to_datetime(ae_df['event_timestamp'], errors='coerce')

# Group events by session
session_map = {}
for _, row in ae_df.iterrows():
    sid = row['session_id']
    event = {
        "event_id": row['event_id'],
        "event_type": row['event_type'],
        "event_timestamp": row['event_timestamp'].isoformat() if pd.notna(row['event_timestamp']) else None,
        "order_id": row['order_id'] if pd.notna(row['order_id']) else None,
        "api_latency_ms": int(row['api_latency_ms']),
        "success_flag": bool(row['success_flag']),
        "zone_context": row['zone_context'],
    }
    if sid not in session_map:
        session_map[sid] = {
            "session_id": sid,
            "customer_id": row['customer_id'],
            "device_type": row['device_type'],
            "events": [],
            "event_count": 0,
            "failed_events": 0,
            "avg_latency_ms": 0,
            "created_at": datetime.utcnow()
        }
    session_map[sid]['events'].append(event)
    session_map[sid]['event_count'] += 1
    if not bool(row['success_flag']):
        session_map[sid]['failed_events'] += 1

# Compute session-level aggregates
for sid, doc in session_map.items():
    latencies = [e['api_latency_ms'] for e in doc['events']]
    doc['avg_latency_ms'] = round(sum(latencies) / len(latencies), 2) if latencies else 0
    doc['has_escalation'] = any(e['event_type'] == 'chat_escalated' for e in doc['events'])
    doc['has_cancel_attempt'] = any(e['event_type'] == 'cancel_attempt' for e in doc['events'])
    doc['has_payment_retry'] = any(e['event_type'] == 'payment_retry' for e in doc['events'])

session_docs = list(session_map.values())
db.app_session_events.drop()
result = db.app_session_events.insert_many(session_docs)
print(f"  Inserted {len(result.inserted_ids)} app_session_events documents")

# 7. CRUD OPERATIONS
print("\n=== CRUD OPERATIONS ===")

# CREATE - Add a new complaint to an existing customer
new_complaint = {
    "complaint_id": "CP_TEST_001",
    "order_id": "O00999",
    "complaint_type": "Delay",
    "channel": "App",
    "severity": "High",
    "created_at": datetime.utcnow().isoformat(),
    "status": "Open",
    "resolution_days": None,
    "compensation_amount": None
}
db.customer_cases.update_one(
    {"customer_id": "C0001"},
    {"$push": {"complaint_history": new_complaint},
     "$inc": {"total_complaints": 1}}
)
print("CREATE: Pushed new complaint to C0001")

# READ - Find all high-severity, open complaints
high_sev_open = list(db.customer_cases.find(
    {"complaint_history": {"$elemMatch": {"severity": "High", "status": "Open"}}},
    {"customer_id": 1, "customer_type": 1, "total_complaints": 1, "_id": 0}
).limit(5))
print(f"READ: {len(high_sev_open)} customers with High severity open complaints (showing 5):")
for c in high_sev_open:
    print(f"  {c}")

# UPDATE - Mark all 'Open' Delay complaints as 'InReview' for a customer
result = db.customer_cases.update_many(
    {"complaint_history.complaint_type": "Delay",
     "complaint_history.status": "Open"},
    {"$set": {"complaint_history.$[elem].status": "InReview"}},
    array_filters=[{"elem.complaint_type": "Delay", "elem.status": "Open"}]
)
print(f"UPDATE: Modified {result.modified_count} customer documents (Delay->InReview)")

# DELETE - Remove test complaint created above
db.customer_cases.update_one(
    {"customer_id": "C0001"},
    {"$pull": {"complaint_history": {"complaint_id": "CP_TEST_001"}},
     "$inc": {"total_complaints": -1}}
)
print("DELETE: Removed test complaint from C0001")

# 8. AGGREGATION PIPELINE QUERIES
print("\n=== AGGREGATION PIPELINE QUERIES ===")

# Aggregation 1: Average rating and failure rate by hub
print("\n--- Agg 1: Delivery performance by hub ---")
pipeline_hub = [
    {"$group": {
        "_id": "$hub_id",
        "total_deliveries": {"$sum": 1},
        "failed": {"$sum": {"$cond": [{"$eq": ["$delivery_status", "Failed"]}, 1, 0]}},
        "avg_rating": {"$avg": "$customer_rating_post_delivery"},
        "avg_overrides": {"$avg": "$manual_route_override_count"},
        "avg_cost": {"$avg": "$fuel_or_charge_cost"}
    }},
    {"$addFields": {
        "failure_pct": {"$round": [{"$multiply": [{"$divide": ["$failed", "$total_deliveries"]}, 100]}, 1]}
    }},
    {"$sort": {"failure_pct": -1}}
]
hub_results = list(db.delivery_operations.aggregate(pipeline_hub))
for r in hub_results:
    r.pop('_id_fields', None)
    print(f"  Hub {r['_id']}: {r['total_deliveries']} deliveries, {r['failure_pct']}% failed, avg rating {round(r['avg_rating'] or 0,2)}")

# Aggregation 2: Customer complaint risk profiling
print("\n--- Agg 2: High-risk customers (2+ complaints) ---")
pipeline_risk = [
    {"$match": {"total_complaints": {"$gte": 2}}},
    {"$project": {
        "customer_id": 1, "customer_type": 1, "home_zone": 1,
        "total_complaints": 1,
        "total_compensation": {"$sum": "$complaint_history.compensation_amount"},
        "open_complaints": {
            "$size": {"$filter": {
                "input": "$complaint_history",
                "cond": {"$eq": ["$$this.status", "Open"]}
            }}
        }
    }},
    {"$sort": {"total_complaints": -1}},
    {"$limit": 8}
]
risk_results = list(db.customer_cases.aggregate(pipeline_risk))
for r in risk_results:
    print(f"  {r['customer_id']} ({r['customer_type']}, {r['home_zone']}): "
          f"{r['total_complaints']} complaints, {r['open_complaints']} open, "
          f"£{r.get('total_compensation') or 0:.2f} compensation")

# Aggregation 3: Delivery incident type breakdown
print("\n--- Agg 3: Incident types in failed deliveries ---")
pipeline_inc = [
    {"$match": {"delivery_status": "Failed", "incident_count": {"$gt": 0}}},
    {"$unwind": "$incidents"},
    {"$group": {
        "_id": "$incidents.incident_type",
        "count": {"$sum": 1},
        "avg_severity_hours": {"$avg": "$incidents.resolved_hours"}
    }},
    {"$sort": {"count": -1}}
]
inc_results = list(db.delivery_operations.aggregate(pipeline_inc))
for r in inc_results:
    print(f"  {r['_id']}: {r['count']} occurrences, avg resolve {round(r['avg_severity_hours'] or 0,1)}h")

# Aggregation 4: App session quality analysis
print("\n--- Agg 4: App sessions with high latency or failures ---")
pipeline_app = [
    {"$match": {"$or": [{"avg_latency_ms": {"$gt": 600}}, {"failed_events": {"$gt": 0}}]}},
    {"$group": {
        "_id": "$device_type",
        "total_sessions": {"$sum": 1},
        "avg_latency": {"$avg": "$avg_latency_ms"},
        "sessions_with_escalation": {"$sum": {"$cond": ["$has_escalation", 1, 0]}},
        "sessions_with_cancel": {"$sum": {"$cond": ["$has_cancel_attempt", 1, 0]}}
    }},
    {"$sort": {"total_sessions": -1}}
]
app_results = list(db.app_session_events.aggregate(pipeline_app))
for r in app_results:
    print(f"  {r['_id']}: {r['total_sessions']} problem sessions, avg latency {round(r['avg_latency'],0)}ms")

# 9. QUERY OPTIMISATION: INDEXING
print("\n=== QUERY OPTIMISATION: INDEXING ===")

# Create compound index on delivery_operations
db.delivery_operations.create_index(
    [("delivery_status", ASCENDING), ("hub_id", ASCENDING)],
    name="idx_status_hub"
)
db.delivery_operations.create_index(
    [("driver_snapshot.driver_id", ASCENDING)],
    name="idx_driver_id"
)
db.delivery_operations.create_index(
    [("dispatch_time", DESCENDING)],
    name="idx_dispatch_time_desc"
)
print("  Created indexes on delivery_operations: status+hub, driver_id, dispatch_time")

# Create index on customer_cases for complaint lookups
db.customer_cases.create_index(
    [("customer_id", ASCENDING)],
    name="idx_customer_id",
    unique=True
)
db.customer_cases.create_index(
    [("total_complaints", DESCENDING), ("customer_type", ASCENDING)],
    name="idx_complaint_count_type"
)
print("  Created indexes on customer_cases: customer_id (unique), complaint_count+type")

# Create index on app_session_events
db.app_session_events.create_index(
    [("customer_id", ASCENDING), ("avg_latency_ms", DESCENDING)],
    name="idx_customer_latency"
)
db.app_session_events.create_index(
    [("has_escalation", ASCENDING)],
    name="idx_has_escalation"
)
print("  Created indexes on app_session_events: customer+latency, has_escalation")

# 10. EXPLAIN PLAN – BEFORE AND AFTER INDEX
print("\n=== EXPLAIN PLAN ANALYSIS ===")

# Simulate before-index scenario with a hint to ignore indexes
explain_hint_no_idx = db.delivery_operations.find(
    {"delivery_status": "Failed", "hub_id": "H05"}
).hint({"$natural": 1}).explain()
print(f"\nBEFORE index (COLLSCAN): stage={explain_hint_no_idx['queryPlanner']['winningPlan']['stage']}, "
      f"docsExamined approx={explain_hint_no_idx.get('executionStats',{}).get('totalDocsExamined','N/A')}")

# After – use the compound index
explain_with_idx = db.delivery_operations.find(
    {"delivery_status": "Failed", "hub_id": "H05"}
).hint("idx_status_hub").explain()
print(f"AFTER  index (IXSCAN): stage={explain_with_idx['queryPlanner']['winningPlan']['stage']}")

print("\nMongoDB Atlas development and optimisation complete.")
client.close()
