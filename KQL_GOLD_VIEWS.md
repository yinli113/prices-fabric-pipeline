# Gold Layer KQL Functions for Dashboards

These functions define the "Gold" layer of our Medallion Architecture. They transform the clean `silver_price_history` into specific business views for our two target audiences: The Shopper and The Retailer.

## 1. The Shopper Dashboard View (`gold_shopper_best_deals`)
**Goal:** Help a consumer find the absolute best place to buy a specific item *right now* based on Price, Delivery Speed, and Seller Trust.

```kusto
.create-or-alter function gold_shopper_best_deals() {
    // 1. Get the absolute latest ingestion time for each product/seller combo
    let latest_times = silver_price_history()
        | summarize max_time = max(ingestion_time) by product_id, seller;
        
    // 2. Take ALL history and join it with latest_times (using leftouter so we keep ALL history)
    silver_price_history()
    | join kind=leftouter latest_times on product_id, seller
    | extend is_latest = iff(ingestion_time == max_time, true, false)
    
    // 3. Force the messy titles into perfectly clean "Buckets"
    | extend clean_category = case(
        tolower(offer_title) contains "spider-man" or tolower(offer_title) contains "spiderman", "PlayStation 5 Spider-Man Game",
        tolower(offer_title) contains "forza", "Xbox Series X Forza Game",
        tolower(offer_title) contains "controller" or tolower(offer_title) contains "dualsense", "PlayStation 5 Controller",
        tolower(offer_title) contains "headset" or tolower(offer_title) contains "earbuds", "Gaming Headset",
        tolower(offer_title) contains "remote", "Media Remote",
        tolower(product_title) contains "xbox", "Xbox Series X Console",
        tolower(product_title) contains "playstation" or tolower(product_title) contains "ps5", "PlayStation 5 Console",
        "Other Accessory"
    )
    
    // 4. Calculate Max Price per CLEAN CATEGORY per HOUR to filter out junk accurately over time
    | extend join_hour = bin(ingestion_time, 1h)
    | join kind=inner (
        silver_price_history()
        | extend temp_category = case(
            tolower(offer_title) contains "spider-man" or tolower(offer_title) contains "spiderman", "PlayStation 5 Spider-Man Game",
            tolower(offer_title) contains "forza", "Xbox Series X Forza Game",
            tolower(offer_title) contains "controller" or tolower(offer_title) contains "dualsense", "PlayStation 5 Controller",
            tolower(offer_title) contains "headset" or tolower(offer_title) contains "earbuds", "Gaming Headset",
            tolower(offer_title) contains "remote", "Media Remote",
            tolower(product_title) contains "xbox", "Xbox Series X Console",
            tolower(product_title) contains "playstation" or tolower(product_title) contains "ps5", "PlayStation 5 Console",
            "Other Accessory"
        )
        | extend join_hour = bin(ingestion_time, 1h)
        | summarize category_max_price = max(price) by temp_category, join_hour
    ) on $left.clean_category == $right.temp_category, $left.join_hour == $right.join_hour
    
    // 5. Keep only the real items (within 30% of max price)
    | extend variance_from_max = (category_max_price - price) / category_max_price
    | where variance_from_max <= 0.30
    
    // 6. Calculate the "Value Score" (0-100)
    | extend rating_penalty = iff(isnotnull(rating), (5.0 - rating) * 20.0, 20.0) // Penalize missing ratings
    | extend delivery_penalty = 10.0 // Penalize missing delivery info
    | extend raw_value_score = 100.0 - rating_penalty - delivery_penalty
    | extend value_score = max_of(0.0, raw_value_score) // Ensure it doesn't go below 0
    
    // 7. Create powerful Time-Series columns for Power BI
    | extend 
        date_recorded = bin(ingestion_time, 1d),
        hour_recorded = bin(ingestion_time, 1h),
        time_label = format_datetime(ingestion_time, 'yyyy-MM-dd HH:mm') // A text string Power BI CANNOT hide!
        
    // 8. Output ONE master table that has EVERYTHING
    | project 
        product = clean_category,
        seller,
        true_total_cost, // Price + Delivery Cost
        price,
        delivery_cost = coalesce(delivery_cost, 0.0),
        rating = coalesce(rating, 0.0),
        review_count,
        stock,
        return_days,
        value_score = round(value_score, 1),
        url,
        is_latest, // The magic column! Use this as a filter in Power BI for the Cards!
        date_recorded,
        hour_recorded,
        time_label,
        exact_time = ingestion_time
    | order by exact_time desc, product asc, value_score desc
}
```

## 2. The Retailer Dashboard View (`gold_retailer_price_trends`)
**Goal:** Help a store owner monitor the market average, identify aggressive competitors, and track how prices for a specific category change over time (hourly and daily).

```kusto
.create-or-alter function gold_retailer_price_trends() {
    silver_price_history()
    // 1. Force the messy titles into perfectly clean "Buckets" (Same logic as Shopper view)
    | extend clean_category = case(
        tolower(offer_title) contains "spider-man" or tolower(offer_title) contains "spiderman", "PlayStation 5 Spider-Man Game",
        tolower(offer_title) contains "forza", "Xbox Series X Forza Game",
        tolower(offer_title) contains "controller" or tolower(offer_title) contains "dualsense", "PlayStation 5 Controller",
        tolower(offer_title) contains "headset" or tolower(offer_title) contains "earbuds", "Gaming Headset",
        tolower(offer_title) contains "remote", "Media Remote",
        tolower(product_title) contains "xbox", "Xbox Series X Console",
        tolower(product_title) contains "playstation" or tolower(product_title) contains "ps5", "PlayStation 5 Console",
        "Other Accessory"
    )
    
    // 2. Drop outliers by comparing against the max price for that ingestion time block
    | extend join_hour = bin(ingestion_time, 1h)
    | join kind=inner (
        silver_price_history()
        | extend temp_category = case(
            tolower(offer_title) contains "spider-man" or tolower(offer_title) contains "spiderman", "PlayStation 5 Spider-Man Game",
            tolower(offer_title) contains "forza", "Xbox Series X Forza Game",
            tolower(offer_title) contains "controller" or tolower(offer_title) contains "dualsense", "PlayStation 5 Controller",
            tolower(offer_title) contains "headset" or tolower(offer_title) contains "earbuds", "Gaming Headset",
            tolower(offer_title) contains "remote", "Media Remote",
            tolower(product_title) contains "xbox", "Xbox Series X Console",
            tolower(product_title) contains "playstation" or tolower(product_title) contains "ps5", "PlayStation 5 Console",
            "Other Accessory"
        )
        // Group by category AND a 1-hour window so we get the max price for that specific hour in history
        | extend join_hour = bin(ingestion_time, 1h)
        | summarize category_max_price = max(price) by temp_category, join_hour
    ) on $left.clean_category == $right.temp_category, $left.join_hour == $right.join_hour
    
    | extend variance_from_max = (category_max_price - price) / category_max_price
    | where variance_from_max <= 0.30
    
    // 3. Create powerful Time-Series columns for Power BI to easily chart
    | extend 
        date_recorded = bin(ingestion_time, 1d), // Rounds to the Day (e.g. 2026-04-06)
        hour_recorded = bin(ingestion_time, 1h)  // Rounds to the Hour (e.g. 2026-04-06 14:00)
    
    // 4. Calculate Market Averages for that specific hour
    // We actually don't strictly need a join here if Power BI handles the averages, but providing the raw time-series data is better!
    // Simpler approach: Just output the clean, timestamped data and let Power BI do the averaging!
    | project 
        product = clean_category,
        seller,
        true_total_cost, // Price + Delivery Cost
        price,
        delivery_cost = coalesce(delivery_cost, 0.0),
        stock,
        rating = coalesce(rating, 0.0),
        review_count,
        return_days,
        url,
        date_recorded,
        hour_recorded,
        exact_time = ingestion_time
    | order by exact_time desc
}
```
