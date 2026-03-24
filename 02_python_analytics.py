# NorthStar Urban Mobility – Python Data Processing & Analytics
# Databases and Analytics Assignment

# Install required libraries (Google Colab)
# !pip install pandas numpy matplotlib seaborn scipy

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# 1. DATA LOADING

dfs = {}
files = ['customers','orders','deliveries','drivers','vehicles','hubs','complaints','incidents','app_events']
for f in files:
 dfs[f] = pd.read_csv(f'{f}.csv')
 print(f"Loaded {f}: {dfs[f].shape}")

customers  = dfs['customers']
orders     = dfs['orders']
deliveries = dfs['deliveries']
drivers    = dfs['drivers']
vehicles   = dfs['vehicles']
hubs       = dfs['hubs']
complaints = dfs['complaints']
incidents  = dfs['incidents']
app_events = dfs['app_events']

# 2. DATA CLEANING & FEATURE ENGINEERING

def standardise_zone(z):
    """Normalise inconsistent zone naming across all datasets."""
    if pd.isna(z): return z
    z = str(z).strip()
    mapping = {
        'airport':'Airport', 'AIRPORT':'Airport',
        'central':'Central', 'CENTRAL':'Central', 'Ctr':'Central',
        'east':'East', 'EAST':'East',
        'north':'North', 'NORTH':'North',
        'south':'South', 'SOUTH':'South',
        'west':'West', 'WEST':'West',
        'riverside':'Riverside', 'RiverSide':'Riverside', 'RIVERSIDE':'Riverside'
    }
    return mapping.get(z, z.title())

# Apply zone standardisation
for df_name in ['customers','orders','drivers','vehicles']:
    df = dfs[df_name]
    for col in df.columns:
 if 'zone' in col.lower():
dfs[df_name][col] = df[col].apply(standardise_zone)

dfs['app_events']['zone_context'] = dfs['app_events']['zone_context'].apply(standardise_zone)

# Reassign references
customers = dfs['customers']; orders = dfs['orders']; deliveries = dfs['deliveries']
drivers = dfs['drivers']; vehicles = dfs['vehicles']; app_events = dfs['app_events']

# Missing value treatment
# 1. Customers: fill preferred_channel with "Unknown", loyalty_score with median
customers['preferred_channel'] = customers['preferred_channel'].fillna('Unknown')
customers['loyalty_score'] = customers['loyalty_score'].fillna(customers['loyalty_score'].median())
print(f"\nCustomer missing preferred_channel filled. Loyalty score median: {customers['loyalty_score'].median():.2f}")

# 2. Orders: fill booking_channel with mode
customers['preferred_channel'] = customers['preferred_channel'].fillna(
    customers['preferred_channel'].mode()[0])

# 3. Drivers: fill training_score with median
drivers['training_score'] = drivers['training_score'].fillna(drivers['training_score'].median())

# 4. Vehicles: fill battery_health_pct with per-type median
vehicles['battery_health_pct'] = vehicles.groupby('vehicle_type')['battery_health_pct'].transform(lambda x: x.fillna(x.median()))

# 5. Incidents: fill resolved_hours with median
incidents['resolved_hours'] = incidents['resolved_hours'].fillna(incidents['resolved_hours'].median())

# Datetime parsing and feature engineering
deliveries['dispatch_time'] = pd.to_datetime(deliveries['dispatch_time'], errors='coerce')
deliveries['delivery_completed_at'] = pd.to_datetime(deliveries['delivery_completed_at'], errors='coerce')
deliveries['actual_duration_hrs'] = (
    deliveries['delivery_completed_at'] - deliveries['dispatch_time']).dt.total_seconds() / 3600

deliveries['cost_per_km'] = deliveries['fuel_or_charge_cost'] /     deliveries['route_distance_km'].replace(0, np.nan)

deliveries['dispatch_hour'] = deliveries['dispatch_time'].dt.hour
deliveries['dispatch_month'] = deliveries['dispatch_time'].dt.month

# Flag high-override deliveries
deliveries['high_override'] = (deliveries['manual_route_override_count'] >= 3).astype(int)

# Customer risk score (composite metric)
merged_comp = complaints.groupby('customer_id').agg(
    complaint_count=('complaint_id', 'count'),
    total_comp=('compensation_amount', 'sum')
).reset_index()
customers = customers.merge(merged_comp, on='customer_id', how='left')
customers['complaint_count'] = customers['complaint_count'].fillna(0)
customers['total_comp'] = customers['total_comp'].fillna(0)

print("\nFeature engineering complete.")
print(f"New delivery features: actual_duration_hrs, cost_per_km, dispatch_hour, high_override")
print(f"New customer features: complaint_count, total_comp")

# 3. DESCRIPTIVE STATISTICS

print("\n=== DESCRIPTIVE STATISTICS ===")
print("\n--- Deliveries ---")
print(deliveries[['route_distance_km','customer_rating_post_delivery',
'fuel_or_charge_cost','actual_duration_hrs']].describe().round(3))

print("\n--- Orders ---")
print(orders[['order_value','promised_window_hours']].describe().round(3))

print("\n--- Delivery Status Distribution ---")
status_dist = deliveries['delivery_status'].value_counts()
for s, c in status_dist.items():
    print(f"  {s}: {c} ({c/len(deliveries)*100:.1f}%)")

# 4. ANALYTICAL FINDINGS
# Finding 1: Zone-level failure analysis
print("\n=== Finding 1: Delivery Outcomes by Pickup Zone ===")
merged = deliveries.merge(orders, on='order_id', how='left')
zone_fail = merged.groupby('pickup_zone')['delivery_status'].value_counts(normalize=True).unstack().fillna(0) * 100
zone_fail.columns.name = None
print(zone_fail.round(1))

# Finding 2: Hub-level performance
print("\n=== Finding 2: Hub Failure Rates ===")
hub_perf = deliveries.groupby('hub_id').agg(
    total=('delivery_id', 'count'),
    failed=('delivery_status', lambda x: (x=='Failed').sum()),
    avg_rating=('customer_rating_post_delivery', 'mean'),
    avg_overrides=('manual_route_override_count', 'mean')
).reset_index()
hub_perf['failure_pct'] = (hub_perf['failed'] / hub_perf['total'] * 100).round(1)
hub_perf = hub_perf.merge(hubs[['hub_id','hub_name']], on='hub_id', how='left')
print(hub_perf[['hub_name','total','failure_pct','avg_rating','avg_overrides']].sort_values('failure_pct', ascending=False).round(3))

# Finding 3: Vehicle risk assessment
print("\n=== Finding 3: High-Risk Vehicles ===")
high_risk_veh = vehicles[
    (vehicles['maintenance_status'] == 'InRepair') & 
    (vehicles['odometer_km'] > 150000)
].copy()
print(f"Vehicles in InRepair with odometer >150k km: {len(high_risk_veh)}")
print(high_risk_veh[['vehicle_id','vehicle_type','odometer_km','battery_health_pct']].sort_values('odometer_km', ascending=False).head(8))

# Finding 4: Manual override correlation
print("\n=== Finding 4: Manual Override Analysis ===")
override_status = deliveries.groupby('delivery_status')['manual_route_override_count'].agg(['mean','sum'])
print(override_status.round(3))
corr_val, p_val = stats.pearsonr(
    deliveries['manual_route_override_count'],
    deliveries['customer_rating_post_delivery'].fillna(deliveries['customer_rating_post_delivery'].median())
)
print(f"\nCorrelation (overrides vs rating): r={corr_val:.4f}, p={p_val:.4f}")

# Finding 5: Complaint clustering
print("\n=== Finding 5: Complaint Type & Severity Backlog ===")
print(complaints.groupby(['complaint_type','severity'])['complaint_id'].count().unstack().fillna(0).astype(int))

# Finding 6: App latency issues
print("\n=== Finding 6: App Event API Latency (ms) ===")
print(app_events.groupby('event_type')['api_latency_ms'].agg(['mean','median','max']).sort_values('mean', ascending=False).round(1))

# 5. VISUALISATIONS
fig_colors = ['#2980b9','#e74c3c','#f39c12','#27ae60','#8e44ad','#16a085','#d35400','#2c3e50']
status_colors = {'OnTime':'#2ecc71','Delayed':'#f39c12','Failed':'#e74c3c'}

#  Figure 1: Delivery Status Pie + Bar 
fig, axes = plt.subplots(1, 2, figsize=(13, 5))
fig.suptitle('NorthStar Delivery Performance Overview', fontsize=15, fontweight='bold')

status_counts = deliveries['delivery_status'].value_counts()
axes[0].pie(status_counts.values, labels=status_counts.index,
            colors=[status_colors[s] for s in status_counts.index],
            autopct='%1.1f%%', startangle=140)
axes[0].set_title('Delivery Status Split', fontsize=12, fontweight='bold')

axes[1].bar(status_counts.index, status_counts.values,
            color=[status_colors[s] for s in status_counts.index], edgecolor='white')
for i, (idx, val) in enumerate(status_counts.items()):
    axes[1].text(i, val+5, str(val), ha='center', fontweight='bold')
axes[1].set_ylabel('Count'); axes[1].set_title('Count by Status', fontsize=12, fontweight='bold')
axes[1].spines[['top','right']].set_visible(False)
plt.tight_layout(); plt.savefig('fig1_delivery_overview.png', dpi=150, bbox_inches='tight'); plt.show()

#  Figure 2: Zone failure heatmap 
fig, ax = plt.subplots(figsize=(9, 5))
zone_fail_matrix = zone_fail[['Failed','Delayed','OnTime']] if 'Failed' in zone_fail.columns else zone_fail
sns.heatmap(zone_fail_matrix, annot=True, fmt='.1f', cmap='RdYlGn_r',
            linewidths=0.5, ax=ax, cbar_kws={'label': '%'})
ax.set_title('Delivery Outcome % by Pickup Zone', fontsize=13, fontweight='bold')
ax.set_xlabel('Delivery Status'); ax.set_ylabel('Pickup Zone')
plt.tight_layout(); plt.savefig('fig2_zone_heatmap.png', dpi=150, bbox_inches='tight'); plt.show()

#  Figure 3: Hub performance
fig, ax = plt.subplots(figsize=(9, 5))
hub_sorted = hub_perf.sort_values('failure_pct', ascending=True)
bars = ax.barh(hub_sorted['hub_name'], hub_sorted['failure_pct'],
color=['#e74c3c' if v > 17 else '#f39c12' if v > 12 else '#2ecc71'
         for v in hub_sorted['failure_pct']])
for bar, val in zip(bars, hub_sorted['failure_pct']):
    ax.text(bar.get_width()+0.2, bar.get_y()+bar.get_height()/2,
            f'{val:.1f}%', va='center', fontweight='bold')
ax.set_xlabel('Failure Rate (%)'); ax.set_title('Delivery Failure Rate by Hub', fontsize=13, fontweight='bold')
ax.spines[['top','right']].set_visible(False)
plt.tight_layout(); plt.savefig('fig3_hub_failure.png', dpi=150, bbox_inches='tight'); plt.show()

# Figure 4: Complaint types bar
fig, ax = plt.subplots(figsize=(8, 5))
comp_counts = complaints['complaint_type'].value_counts()
ax.barh(comp_counts.index, comp_counts.values, color=fig_colors[:len(comp_counts)])
for i, (v, idx) in enumerate(zip(comp_counts.values, comp_counts.index)):
    ax.text(v+0.3, i, str(v), va='center', fontweight='bold')
ax.set_xlabel('Number of Complaints')
ax.set_title('Complaint Volume by Type', fontsize=13, fontweight='bold')
ax.spines[['top','right']].set_visible(False)
plt.tight_layout(); plt.savefig('fig4_complaints.png', dpi=150, bbox_inches='tight'); plt.show()

#  Figure 5: Driver training vs rating scatter 
fig, ax = plt.subplots(figsize=(7, 5))
emp_palette = {'FullTime':'#2980b9','PartTime':'#e74c3c','Contract':'#f39c12'}
for etype, grp in drivers.dropna(subset=['training_score','driver_rating']).groupby('employment_type'):
    ax.scatter(grp['training_score'], grp['driver_rating'], label=etype,
               color=emp_palette.get(etype,'grey'), alpha=0.7, s=60)
m, b = np.polyfit(drivers.dropna(subset=['training_score','driver_rating'])['training_score'],
                  drivers.dropna(subset=['training_score','driver_rating'])['driver_rating'], 1)
x_line = np.linspace(drivers['training_score'].min(), drivers['training_score'].max(), 100)
r = np.corrcoef(drivers.dropna(subset=['training_score','driver_rating'])['training_score'],
                drivers.dropna(subset=['training_score','driver_rating'])['driver_rating'])[0,1]
ax.plot(x_line, m*x_line+b, '--', color='grey', linewidth=1.5, label=f'Trend r={r:.3f}')
ax.set_xlabel('Training Score'); ax.set_ylabel('Driver Rating')
ax.set_title('Driver Training Score vs Rating', fontsize=13, fontweight='bold')
ax.legend(fontsize=9); ax.spines[['top','right']].set_visible(False)
plt.tight_layout(); plt.savefig('fig5_driver_scatter.png', dpi=150, bbox_inches='tight'); plt.show()

#  Figure 6: API Latency boxplot 
fig, ax = plt.subplots(figsize=(10, 5))
event_order = app_events.groupby('event_type')['api_latency_ms'].mean().sort_values(ascending=False).index
sns.boxplot(data=app_events, x='event_type', y='api_latency_ms', order=event_order,
            palette=fig_colors[:len(event_order)], ax=ax)
ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha='right')
ax.set_xlabel('Event Type'); ax.set_ylabel('API Latency (ms)')
ax.set_title('App API Latency Distribution by Event Type', fontsize=13, fontweight='bold')
ax.spines[['top','right']].set_visible(False)
plt.tight_layout(); plt.savefig('fig6_api_latency.png', dpi=150, bbox_inches='tight'); plt.show()

#  Figure 7: Order value by service type
fig, ax = plt.subplots(figsize=(8, 5))
service_types = orders['service_type'].unique()
data_by_service = [orders[orders['service_type']==s]['order_value'].dropna().values for s in service_types]
bp = ax.boxplot(data_by_service, tick_labels=service_types, patch_artist=True, notch=False)
for patch, color in zip(bp['boxes'], fig_colors):
    patch.set_facecolor(color); patch.set_alpha(0.7)
ax.set_ylabel('Order Value (£)')
ax.set_title('Order Value Distribution by Service Type', fontsize=13, fontweight='bold')
ax.spines[['top','right']].set_visible(False)
plt.tight_layout(); plt.savefig('fig7_order_value.png', dpi=150, bbox_inches='tight'); plt.show()

#  Figure 8: Vehicle battery health distribution 
fig, ax = plt.subplots(figsize=(8, 5))
for vtype, grp in vehicles.groupby('vehicle_type'):
    ax.hist(grp['battery_health_pct'].dropna(), bins=15, alpha=0.6, label=vtype)
ax.set_xlabel('Battery Health (%)'); ax.set_ylabel('Count')
ax.set_title('Battery Health Distribution by Vehicle Type', fontsize=13, fontweight='bold')
ax.legend(); ax.spines[['top','right']].set_visible(False)
plt.tight_layout(); plt.savefig('fig8_battery_health.png', dpi=150, bbox_inches='tight'); plt.show()

print("\nAll Python visualisations complete.")

# 6. STATISTICAL TESTS

print("\n=== STATISTICAL TESTS ===")

# Chi-square: complaint type vs severity
contingency = pd.crosstab(complaints['complaint_type'], complaints['severity'])
chi2, p_val, dof, expected = stats.chi2_contingency(contingency)
print(f"\nChi-Square Test (complaint_type vs severity):")
print(f"  chi2={chi2:.4f}, p={p_val:.4f}, dof={dof}")
print(f"  Interpretation: {'Significant association' if p_val < 0.05 else 'No significant association'}")

# Kruskal-Wallis: rating across delivery statuses
groups = [deliveries[deliveries['delivery_status']==s]['customer_rating_post_delivery'].dropna()
          for s in ['OnTime','Delayed','Failed']]
H, p = stats.kruskal(*groups)
print(f"\nKruskal-Wallis Test (rating by delivery status):")
print(f"  H={H:.4f}, p={p:.6f}")
print(f"  Interpretation: {'Significant difference' if p < 0.05 else 'No significant difference'} in ratings across delivery statuses")

# Spearman: override count vs rating
sp_r, sp_p = stats.spearmanr(
    deliveries['manual_route_override_count'],
    deliveries['customer_rating_post_delivery'].fillna(deliveries['customer_rating_post_delivery'].median()))
print(f"\nSpearman Correlation (overrides vs rating):")
print(f"  rho={sp_r:.4f}, p={sp_p:.4f}")
print(f"  Interpretation: {'Significant' if sp_p < 0.05 else 'Not significant'} negative relationship")
