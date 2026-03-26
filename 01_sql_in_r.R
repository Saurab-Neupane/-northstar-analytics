# NorthStar Urban Mobility – SQL in R Analytics
# Databases and Analytics Assignment

# Install and load required packages
if (!require("sqldf")) install.packages("sqldf", quietly=TRUE)
if (!require("ggplot2")) install.packages("ggplot2", quietly=TRUE)
if (!require("dplyr")) install.packages("dplyr", quietly=TRUE)
if (!require("lubridate")) install.packages("lubridate", quietly=TRUE)

library(sqldf)
library(ggplot2)
library(dplyr)
library(lubridate)

# 1. LOAD AND CLEAN DATA
customers  <- read.csv("customers.csv", stringsAsFactors=FALSE)
orders  <- read.csv("orders.csv",  stringsAsFactors=FALSE)
deliveries <- read.csv("deliveries.csv", stringsAsFactors=FALSE)
drivers  <- read.csv("drivers.csv", stringsAsFactors=FALSE)
vehicles <- read.csv("vehicles.csv", stringsAsFactors=FALSE)
hubs  <- read.csv("hubs.csv", stringsAsFactors=FALSE)
complaints <- read.csv("complaints.csv", stringsAsFactors=FALSE)
incidents  <- read.csv("incidents.csv",  stringsAsFactors=FALSE)
app_events <- read.csv("app_events.csv", stringsAsFactors=FALSE)

# Standardise zone values (case-insensitive normalisation)
standardise_zone <- function(z) {
  z <- trimws(z)
  z <- tolower(z)
  zones <- c(airport="Airport", central="Central", ctr="Central",
   east="East", north="North", south="South",
  west="West", riverside="Riverside", riverside="Riverside")
  result <- zones[z]
  ifelse(is.na(result), tools::toTitleCase(z), result)
}

for (df_name in c("customers","orders","drivers","vehicles")) {
  df <- get(df_name)
  zone_cols <- grep("zone", names(df), ignore.case=TRUE, value=TRUE)
  for (col in zone_cols) {
    df[[col]] <- standardise_zone(df[[col]])
  }
  assign(df_name, df)
}

# Handle missing values
customers$preferred_channel[is.na(customers$preferred_channel)] <- "Unknown"
customers$loyalty_score[is.na(customers$loyalty_score)] <- 
  median(customers$loyalty_score, na.rm=TRUE)
drivers$training_score[is.na(drivers$training_score)] <- 
  median(drivers$training_score, na.rm=TRUE)

cat("Data loaded and cleaned.\n")
cat("Customers:", nrow(customers), "| Orders:", nrow(orders), 
    "| Deliveries:", nrow(deliveries), "\n")

# 2. SQL QUERIES IN R
#  SQL Query 1: Delivery failure rates by pickup zone 
cat("\n SQL Query 1: Delivery Failure Rate by Zone \n")
q1 <- sqldf("
  SELECT o.pickup_zone,
     COUNT(*) AS total_deliveries,
    SUM(CASE WHEN d.delivery_status = 'Failed' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN d.delivery_status = 'Delayed' THEN 1 ELSE 0 END) AS delayed,
    ROUND(100.0 * SUM(CASE WHEN d.delivery_status = 'Failed' THEN 1 ELSE 0 END) 
       / COUNT(*), 2) AS failure_pct,
    ROUND(AVG(d.customer_rating_post_delivery), 3) AS avg_rating
  FROM deliveries d
  JOIN orders o ON d.order_id = o.order_id
  GROUP BY o.pickup_zone
  ORDER BY failure_pct DESC
")
print(q1)

#  SQL Query 2: High-value repeat complainers 
cat("\n SQL Query 2: Repeat Complainers Joined with Customer Data \n")
q2 <- sqldf("
  SELECT c.customer_id,
         cu.customer_type,
         cu.home_zone,
         COUNT(c.complaint_id) AS complaint_count,
         SUM(c.compensation_amount) AS total_compensation,
         GROUP_CONCAT(DISTINCT c.complaint_type) AS complaint_types
  FROM complaints c
  JOIN customers cu ON c.customer_id = cu.customer_id
  GROUP BY c.customer_id
  HAVING complaint_count >= 2
  ORDER BY complaint_count DESC, total_compensation DESC
  LIMIT 10
")
print(q2)

#  SQL Query 3: Hub-level operational overview 
cat("\n SQL Query 3: Hub Performance Overview \n")
q3 <- sqldf("
  SELECT d.hub_id,
         h.hub_name,
         h.zone,
         COUNT(d.delivery_id) AS total_deliveries,
         ROUND(AVG(d.customer_rating_post_delivery), 3) AS avg_rating,
         ROUND(AVG(d.manual_route_override_count), 3) AS avg_overrides,
         ROUND(AVG(d.fuel_or_charge_cost), 2) AS avg_cost,
         SUM(CASE WHEN d.delivery_status = 'Failed' THEN 1 ELSE 0 END) AS failed_count,
         ROUND(100.0 * SUM(CASE WHEN d.delivery_status = 'Failed' THEN 1 ELSE 0 END) 
               / COUNT(*), 1) AS failure_rate_pct
  FROM deliveries d
  JOIN hubs h ON d.hub_id = h.hub_id
  GROUP BY d.hub_id
  ORDER BY failure_rate_pct DESC
")
print(q3)

# SQL Query 4: Driver performance with incident history 
cat("\n SQL Query 4: Drivers with Incidents and Low Ratings \n")
q4 <- sqldf("
  SELECT d.driver_id,
         d.employment_type,
         d.base_zone,
         d.years_experience,
         d.driver_rating,
         COUNT(del.delivery_id) AS deliveries_made,
         SUM(CASE WHEN del.delivery_status = 'Failed' THEN 1 ELSE 0 END) AS failed_deliveries,
         COUNT(i.incident_id) AS incidents_linked
  FROM drivers d
  LEFT JOIN deliveries del ON d.driver_id = del.driver_id
  LEFT JOIN incidents i ON del.delivery_id = i.delivery_id
  GROUP BY d.driver_id
  HAVING failed_deliveries > 2
  ORDER BY failed_deliveries DESC, incidents_linked DESC
  LIMIT 10
")
print(q4)

# SQL Query 5: Service profitability by type 
cat("\n SQL Query 5: Service Type Profitability Analysis \n")
q5 <- sqldf("
  SELECT o.service_type,
         COUNT(o.order_id) AS total_orders,
         ROUND(AVG(o.order_value), 2) AS avg_order_value,
         ROUND(AVG(d.fuel_or_charge_cost), 2) AS avg_cost,
         ROUND(AVG(o.order_value) - AVG(d.fuel_or_charge_cost), 2) AS avg_margin,
         ROUND(AVG(d.customer_rating_post_delivery), 3) AS avg_rating
  FROM orders o
  JOIN deliveries d ON o.order_id = d.order_id
  GROUP BY o.service_type
  ORDER BY avg_margin DESC
")
print(q5)

# 3. OPTIMISED SQL WITH INDEXING CONCEPTS IN R
# Creating an indexed subset for hub-zone query performance
# In a real DB, this represents: CREATE INDEX idx_del_hub ON deliveries(hub_id);
cat("\n SQL Query 6 (Optimised): Zone-to-Zone Demand Matrix \n")
q6 <- sqldf("
  SELECT pickup_zone, dropoff_zone,
         COUNT(*) AS journey_count,
         ROUND(AVG(order_value), 2) AS avg_value
  FROM orders
  WHERE booking_channel IS NOT NULL
  GROUP BY pickup_zone, dropoff_zone
  ORDER BY journey_count DESC
  LIMIT 15
")
print(q6)

# 4. R ANALYTICS (Statistical)
# Parse datetime columns
deliveries$dispatch_time <- ymd_hms(deliveries$dispatch_time)
deliveries$delivery_completed_at <- ymd_hms(deliveries$delivery_completed_at)
deliveries$actual_duration_hrs <- as.numeric(difftime(
  deliveries$delivery_completed_at, deliveries$dispatch_time, units="hours"))

# Pearson correlation matrix
cat("\n Pearson Correlation Matrix (Deliveries) \n")
num_cols <- deliveries[, c("manual_route_override_count","route_distance_km", "customer_rating_post_delivery","fuel_or_charge_cost",
 "actual_duration_hrs")]
num_cols <- num_cols[complete.cases(num_cols), ]
print(round(cor(num_cols, method="pearson"), 4))

# One-way ANOVA: Does delivery zone affect ratings?
cat("\n ANOVA: Rating by Pickup Zone \n")
merged_rd <- merge(deliveries, orders, by="order_id")
anova_model <- aov(customer_rating_post_delivery ~ pickup_zone, data=merged_rd)
print(summary(anova_model))

# T-test: Manual override vs no override on ratings
cat("\n T-Test: Rating with override vs without \n")
with_override <- deliveries$customer_rating_post_delivery[
  deliveries$manual_route_override_count > 0 & !is.na(deliveries$customer_rating_post_delivery)]
without_override <- deliveries$customer_rating_post_delivery[
  deliveries$manual_route_override_count == 0 & !is.na(deliveries$customer_rating_post_delivery)]
print(t.test(with_override, without_override))

# 5. VISUALISATIONS IN R
# Plot 1: Delivery status distribution
ggplot(deliveries, aes(x=delivery_status, fill=delivery_status)) +
  geom_bar() +
  scale_fill_manual(values=c("OnTime"="#2ecc71","Delayed"="#f39c12","Failed"="#e74c3c")) +
  labs(title="Delivery Status Distribution", x="Status", y="Count") +
  theme_minimal() + theme(legend.position="none")

# Plot 2: Hub failure rates
hub_summary <- sqldf("
  SELECT d.hub_id, h.hub_name,
   ROUND(100.0*SUM(CASE WHEN delivery_status='Failed' THEN 1 ELSE 0 END)/COUNT(*),1) AS fail_pct
  FROM deliveries d JOIN hubs h ON d.hub_id=h.hub_id
  GROUP BY d.hub_id ORDER BY fail_pct DESC")

ggplot(hub_summary, aes(x=reorder(hub_name, -fail_pct), y=fail_pct, fill=fail_pct)) +
  geom_col() +
  scale_fill_gradient(low="#f39c12", high="#e74c3c") +
  labs(title="Failure Rate by Hub (%)", x="Hub", y="Failure Rate (%)") +
  theme_minimal() + theme(axis.text.x=element_text(angle=30, hjust=1))

# Plot 3: Complaint type frequency
ggplot(complaints, aes(x=reorder(complaint_type, table(complaint_type)[complaint_type]))) +
  geom_bar(fill="#2980b9") +
  coord_flip() +
  labs(title="Complaints by Type", x="Type", y="Count") +
  theme_minimal()

cat("\nSQL in R analysis complete.\n")
