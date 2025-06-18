from bigquery import get_bigquery_client

def execute_ads_rollup_query():
    client = get_bigquery_client()

    query = """
    MERGE `raw_ads.ads_rollup` AS target
    USING (
    WITH base_data AS (
        SELECT 
        -- Ad grouping fields
        ag.client_id,
        ag.reporting_group,
        ag.platform,
        ag.kpi_event,
        ag.kpi_custom_code,
        ag.kpi_goal,
        ag.budget,
        
        -- Meta ads fields (explicitly select only the ones we need)
        ma.actions,
        ma.cost_per_action_type,
        ma.date_start,
        ma.date_stop,
        ma.ad_name,
        ma.account_id,
        ma.account_name,
        ma.account_currency,
        ma.ad_id,
        ma.adset_id,
        ma.adset_name,
        ma.campaign_name,
        ma.impressions,
        ma.reach,
        ma.frequency,
        ma.spend,
        ma.clicks,
        ma.cpc,
        ma.cpm,
        ma.cpp,
        ma.ctr,
        ma.unique_clicks,
        ma.unique_ctr,
        ma.cost_per_unique_click,
        ma.inline_link_clicks,
        ma.inline_link_click_ctr,
        ma.quality_ranking,
        ma.engagement_rate_ranking,
        ma.conversion_rate_ranking,
        ma.objective,
        ma.optimization_goal
        
        FROM `raw_ads.ad_grouping` ag
        LEFT JOIN `raw_ads.meta_ads` ma
        ON UPPER(ma.adset_name) LIKE CONCAT('%', UPPER(ag.ad_set_contains), '%')
        AND ag.platform = 'Meta'
        WHERE ma.ad_id IS NOT NULL
    ),

    mapped_actions AS (
        SELECT 
        bd.*,
        
        -- Get KPI events to map (prioritize custom_code over event)
        COALESCE(bd.kpi_custom_code, bd.kpi_event) as kpi_string,
        
        -- Split and map target_action_0
        COALESCE(
            map0_account.meta_action_type,
            map0_standard.meta_action_type
        ) as target_action_0,
        
        -- Split and map target_action_1  
        COALESCE(
            map1_account.meta_action_type,
            map1_standard.meta_action_type
        ) as target_action_1,
        
        -- Split and map target_action_2
        COALESCE(
            map2_account.meta_action_type,
            map2_standard.meta_action_type
        ) as target_action_2
        
        FROM base_data bd
        
        -- Map action 0
        LEFT JOIN `raw_ads.kpi_event_mapping` map0_account
        ON map0_account.user_friendly_name = TRIM(SPLIT(COALESCE(bd.kpi_custom_code, bd.kpi_event), ',')[SAFE_OFFSET(0)])
        AND map0_account.ad_account_id = bd.account_id
        
        LEFT JOIN `raw_ads.kpi_event_mapping` map0_standard  
        ON map0_standard.user_friendly_name = TRIM(SPLIT(COALESCE(bd.kpi_custom_code, bd.kpi_event), ',')[SAFE_OFFSET(0)])
        AND map0_standard.ad_account_id = 'all'
        
        -- Map action 1
        LEFT JOIN `raw_ads.kpi_event_mapping` map1_account
        ON map1_account.user_friendly_name = TRIM(SPLIT(COALESCE(bd.kpi_custom_code, bd.kpi_event), ',')[SAFE_OFFSET(1)])
        AND map1_account.ad_account_id = bd.account_id
        
        LEFT JOIN `raw_ads.kpi_event_mapping` map1_standard
        ON map1_standard.user_friendly_name = TRIM(SPLIT(COALESCE(bd.kpi_custom_code, bd.kpi_event), ',')[SAFE_OFFSET(1)])
        AND map1_standard.ad_account_id = 'all'
        
        -- Map action 2
        LEFT JOIN `raw_ads.kpi_event_mapping` map2_account
        ON map2_account.user_friendly_name = TRIM(SPLIT(COALESCE(bd.kpi_custom_code, bd.kpi_event), ',')[SAFE_OFFSET(2)])
        AND map2_account.ad_account_id = bd.account_id
        
        LEFT JOIN `raw_ads.kpi_event_mapping` map2_standard
        ON map2_standard.user_friendly_name = TRIM(SPLIT(COALESCE(bd.kpi_custom_code, bd.kpi_event), ',')[SAFE_OFFSET(2)])
        AND map2_standard.ad_account_id = 'all'
    ),

    final_rollup AS (
        SELECT 
        client_id,
        reporting_group,
        kpi_goal,
        budget,
        platform,
        date_start,
        date_stop as date_end,
        ad_name,
        account_id,
        account_name,
        account_currency,
        ad_id,
        adset_id,
        adset_name,
        campaign_name,
        impressions,
        reach,
        frequency,
        spend,
        clicks,
        cpc,
        cpm,
        cpp,
        ctr,
        unique_clicks,
        unique_ctr,
        cost_per_unique_click,
        inline_link_clicks,
        inline_link_click_ctr,
        
        -- Store the mapped action types
        target_action_0 as action_type_0,
        target_action_1 as action_type_1,
        target_action_2 as action_type_2,
        
        -- Extract action values from RECORD array
        (
            SELECT action.value 
            FROM UNNEST(actions) as action 
            WHERE action.action_type = target_action_0 
            LIMIT 1
        ) as actions_value_0,
        
        (
            SELECT action.value 
            FROM UNNEST(actions) as action 
            WHERE action.action_type = target_action_1 
            LIMIT 1
        ) as actions_value_1,
        
        (
            SELECT action.value 
            FROM UNNEST(actions) as action 
            WHERE action.action_type = target_action_2 
            LIMIT 1
        ) as actions_value_2,
        
        -- Extract cost per action type values from RECORD array
        (
            SELECT cost_action.value 
            FROM UNNEST(cost_per_action_type) as cost_action 
            WHERE cost_action.action_type = target_action_0 
            LIMIT 1
        ) as cost_per_action_type_value_0,
        
        (
            SELECT cost_action.value 
            FROM UNNEST(cost_per_action_type) as cost_action 
            WHERE cost_action.action_type = target_action_1 
            LIMIT 1
        ) as cost_per_action_type_value_1,
        
        (
            SELECT cost_action.value 
            FROM UNNEST(cost_per_action_type) as cost_action 
            WHERE cost_action.action_type = target_action_2 
            LIMIT 1
        ) as cost_per_action_type_value_2,
        
        quality_ranking,
        engagement_rate_ranking,
        conversion_rate_ranking,
        objective,
        optimization_goal,
        
        -- Create unique key for merging
        CONCAT(
            COALESCE(client_id, ''), '_',
            COALESCE(ad_id, ''), '_',
            COALESCE(CAST(date_start AS STRING), '')
        ) as merge_key
        FROM mapped_actions
        QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CONCAT(
            COALESCE(client_id, ''), '_',
            COALESCE(ad_id, ''), '_',
            COALESCE(CAST(date_start AS STRING), '')
        )
        ORDER BY date_start DESC
        ) = 1
    )

    SELECT * FROM final_rollup
    ) AS source

    ON target.merge_key = source.merge_key

    WHEN MATCHED THEN
    UPDATE SET
        client_id = source.client_id,
        reporting_group = source.reporting_group,
        kpi_goal = source.kpi_goal,
        budget = source.budget,
        platform = source.platform,
        date_start = source.date_start,
        date_end = source.date_end,
        ad_name = source.ad_name,
        account_id = source.account_id,
        account_name = source.account_name,
        account_currency = source.account_currency,
        ad_id = source.ad_id,
        adset_id = source.adset_id,
        adset_name = source.adset_name,
        campaign_name = source.campaign_name,
        impressions = source.impressions,
        reach = source.reach,
        frequency = source.frequency,
        spend = source.spend,
        clicks = source.clicks,
        cpc = source.cpc,
        cpm = source.cpm,
        cpp = source.cpp,
        ctr = source.ctr,
        unique_clicks = source.unique_clicks,
        unique_ctr = source.unique_ctr,
        cost_per_unique_click = source.cost_per_unique_click,
        inline_link_clicks = source.inline_link_clicks,
        inline_link_click_ctr = source.inline_link_click_ctr,
        action_type_0 = source.action_type_0,
        action_type_1 = source.action_type_1,
        action_type_2 = source.action_type_2,
        actions_value_0 = source.actions_value_0,
        actions_value_1 = source.actions_value_1,
        actions_value_2 = source.actions_value_2,
        cost_per_action_type_value_0 = source.cost_per_action_type_value_0,
        cost_per_action_type_value_1 = source.cost_per_action_type_value_1,
        cost_per_action_type_value_2 = source.cost_per_action_type_value_2,
        quality_ranking = source.quality_ranking,
        engagement_rate_ranking = source.engagement_rate_ranking,
        conversion_rate_ranking = source.conversion_rate_ranking,
        objective = source.objective,
        optimization_goal = source.optimization_goal

    WHEN NOT MATCHED THEN
    INSERT (
        client_id, reporting_group, kpi_goal, budget, platform, date_start, date_end,
        ad_name, account_id, account_name, account_currency, ad_id, adset_id, adset_name,
        campaign_name, impressions, reach, frequency, spend, clicks, cpc, cpm, cpp, ctr,
        unique_clicks, unique_ctr, cost_per_unique_click, inline_link_clicks, 
        inline_link_click_ctr, action_type_0, action_type_1, action_type_2,
        actions_value_0, actions_value_1, actions_value_2, 
        cost_per_action_type_value_0, cost_per_action_type_value_1, cost_per_action_type_value_2,
        quality_ranking, engagement_rate_ranking, conversion_rate_ranking, objective, optimization_goal, merge_key
    )
    VALUES (
        source.client_id, source.reporting_group, source.kpi_goal, source.budget, 
        source.platform, source.date_start, source.date_end, source.ad_name, 
        source.account_id, source.account_name, source.account_currency, source.ad_id, 
        source.adset_id, source.adset_name, source.campaign_name, source.impressions, 
        source.reach, source.frequency, source.spend, source.clicks, source.cpc, 
        source.cpm, source.cpp, source.ctr, source.unique_clicks, source.unique_ctr, 
        source.cost_per_unique_click, source.inline_link_clicks, source.inline_link_click_ctr, 
        source.action_type_0, source.action_type_1, source.action_type_2,
        source.actions_value_0, source.actions_value_1, source.actions_value_2, 
        source.cost_per_action_type_value_0, source.cost_per_action_type_value_1, source.cost_per_action_type_value_2,
        source.quality_ranking, source.engagement_rate_ranking, source.conversion_rate_ranking, 
        source.objective, source.optimization_goal, source.merge_key
    );
    """

    query_job = client.query(query)
    try:
        query_job.result()  # Wait for the job to complete.
        return True
    except Exception as e:
        print(f"Query execution failed: {e}")
        return False