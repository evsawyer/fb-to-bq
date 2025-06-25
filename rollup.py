from bigquery import get_bigquery_client

def execute_ads_rollup_query():
    client = get_bigquery_client()

    query = """
MERGE `raw_ads.ads_rollup_test` AS target
USING (
WITH kpi_rules_aggregated AS (
    SELECT 
    ag.client_id,
    ag.reporting_group,
    ag.ad_set_contains,
    ag.platform,
    
    -- Aggregate all KPI rules per ad_set_contains
    ARRAY_AGG(
        STRUCT(
            ag.KPI_Type as kpi_type,
            ag.Custom_KPI as custom_kpi
        )
    ) as kpi_rules
    
    FROM `raw_ads.ad_grouping` ag
    WHERE ag.platform = 'Meta'
    GROUP BY 
    ag.client_id, ag.reporting_group, ag.ad_set_contains, ag.platform
),

base_data AS (
    SELECT 
    -- Ad grouping fields
    kr.client_id,
    kr.reporting_group,
    kr.platform,
    kr.kpi_rules,
    kr.ad_set_contains,  -- Add ad_set_contains for validation
    
    -- Meta ads fields
    ma.actions,
    ma.video_play_actions,
    ma.video_p25_watched_actions,
    ma.video_p50_watched_actions, 
    ma.video_p75_watched_actions,
    ma.video_p100_watched_actions,
    ma.video_thruplay_watched_actions,
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
    
    FROM kpi_rules_aggregated kr
    LEFT JOIN `raw_ads.meta_ads` ma
    ON UPPER(ma.adset_name) LIKE CONCAT('%', UPPER(kr.ad_set_contains), '%')
    WHERE ma.ad_id IS NOT NULL
),

-- Extract all custom KPI mappings for each row
custom_mappings AS (
    SELECT 
    bd.client_id,
    bd.ad_id,
    bd.date_start,
    bd.account_id,
    rule.kpi_type,
    rule.custom_kpi,
    COALESCE(
        custom_map.meta_action_type,
        standard_map.meta_action_type
    ) as custom_action_type
    
    FROM base_data bd
    CROSS JOIN UNNEST(bd.kpi_rules) as rule
    LEFT JOIN `raw_ads.kpi_event_mapping` custom_map
    ON custom_map.user_friendly_name = rule.custom_kpi
    AND custom_map.ad_account_id = bd.account_id
    LEFT JOIN `raw_ads.kpi_event_mapping` standard_map
    ON standard_map.user_friendly_name = rule.custom_kpi
    AND standard_map.ad_account_id = 'all'
    WHERE rule.custom_kpi IS NOT NULL
),

-- Pivot custom mappings back to columns
custom_action_types AS (
    SELECT 
    client_id,
    ad_id,
    date_start,
    -- Existing KPI columns
    MAX(CASE WHEN kpi_type = 'Lead' THEN custom_action_type END) as lead_custom_action,
    MAX(CASE WHEN kpi_type = 'Video View' THEN custom_action_type END) as video_view_custom_action,
    MAX(CASE WHEN kpi_type = 'Purchase' THEN custom_action_type END) as purchase_custom_action,
    MAX(CASE WHEN kpi_type = 'Page View' THEN custom_action_type END) as page_view_custom_action,
    MAX(CASE WHEN kpi_type = 'Link Click' THEN custom_action_type END) as link_click_custom_action,
    MAX(CASE WHEN kpi_type = 'Page Engagement' THEN custom_action_type END) as page_engagement_custom_action,
    MAX(CASE WHEN kpi_type = 'Post Engagement' THEN custom_action_type END) as post_engagement_custom_action,
    MAX(CASE WHEN kpi_type = 'Landing Page View' THEN custom_action_type END) as landing_page_view_custom_action,
    MAX(CASE WHEN kpi_type = 'Post Reaction' THEN custom_action_type END) as post_reaction_custom_action,
    MAX(CASE WHEN kpi_type = 'Post Save' THEN custom_action_type END) as post_save_custom_action,
    MAX(CASE WHEN kpi_type = 'Web Lead' THEN custom_action_type END) as web_lead_custom_action,
    -- New IVC metrics
    MAX(CASE WHEN kpi_type = 'Click On Platform' THEN custom_action_type END) as click_on_platform_custom_action,
    MAX(CASE WHEN kpi_type = 'Click To Website' THEN custom_action_type END) as click_to_website_custom_action,
    MAX(CASE WHEN kpi_type = 'Lead Website' THEN custom_action_type END) as lead_website_custom_action,
    MAX(CASE WHEN kpi_type = 'Video View 3sec' THEN custom_action_type END) as video_view_3sec_custom_action,
    MAX(CASE WHEN kpi_type = 'View Content' THEN custom_action_type END) as view_content_custom_action,
    MAX(CASE WHEN kpi_type = 'Add To Cart' THEN custom_action_type END) as add_to_cart_custom_action,
    MAX(CASE WHEN kpi_type = 'Initiate Checkout' THEN custom_action_type END) as initiate_checkout_custom_action,
    MAX(CASE WHEN kpi_type = 'Complete Registration' THEN custom_action_type END) as complete_registration_custom_action
    
    FROM custom_mappings
    GROUP BY client_id, ad_id, date_start
),

final_rollup AS (
    SELECT 
    bd.client_id,
    bd.reporting_group,
    bd.platform,
    bd.date_start,
    bd.date_stop as date_end,
    bd.ad_name,
    bd.account_id,
    bd.account_name,
    bd.account_currency,
    bd.ad_id,
    bd.adset_id,
    bd.adset_name,
    bd.campaign_name,
    bd.impressions,
    bd.reach,
    bd.frequency,
    bd.spend,
    bd.clicks,
    bd.cpc,
    bd.cpm,
    bd.cpp,
    bd.ctr,
    bd.unique_clicks,
    bd.unique_ctr,
    bd.cost_per_unique_click,
    bd.inline_link_clicks,
    bd.inline_link_click_ctr,
    bd.quality_ranking,
    bd.engagement_rate_ranking,
    bd.conversion_rate_ranking,
    bd.objective,
    bd.optimization_goal,
    
    -- Add validation columns
    bd.actions,                     -- Full actions array for validation
    bd.video_play_actions,          -- Video play actions for validation
    bd.video_p50_watched_actions,   -- Video 50% watched actions for validation
    bd.video_p100_watched_actions,  -- Video 100% watched actions for validation
    bd.ad_set_contains,             -- Matching criteria for validation
    
    -- Extract Lead value with custom override
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.lead_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'lead'
            LIMIT 1
        )
    ) as Lead,
    
    -- Extract Video View value with custom override
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.video_view_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'video_view'
            LIMIT 1
        )
    ) as Video_View,
    
    -- Extract Purchase value with custom override
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.purchase_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'purchase'
            LIMIT 1
        )
    ) as Purchase,
    
    -- Extract Page View value with custom override
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.page_view_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'page_view'
            LIMIT 1
        )
    ) as Page_View,
    
    -- Extract Link Click value with custom override
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.link_click_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'link_click'
            LIMIT 1
        )
    ) as Link_Click,
    
    -- Extract Page Engagement value with custom override
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.page_engagement_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'page_engagement'
            LIMIT 1
        )
    ) as Page_Engagement,
    
    -- Extract Post Engagement value with custom override
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.post_engagement_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'post_engagement'
            LIMIT 1
        )
    ) as Post_Engagement,
    
    -- Extract Landing Page View value with custom override
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.landing_page_view_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'landing_page_view'
            LIMIT 1
        )
    ) as Landing_Page_View,
    
    -- Extract Post Reaction value with custom override
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.post_reaction_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'post_reaction'
            LIMIT 1
        )
    ) as Post_Reaction,
    
    -- Extract Post Save value with custom override
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.post_save_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'post_save'
            LIMIT 1
        )
    ) as Post_Save,
    
    -- Extract Web Lead value with custom override
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.web_lead_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'web_lead'
            LIMIT 1
        )
    ) as Web_Lead,
    
    -- Extract video metrics directly from base_data (sourced from meta_ads)
    (
        SELECT action.value 
        FROM UNNEST(bd.video_play_actions) as action 
        WHERE action.action_type = 'video_view'
        LIMIT 1
    ) as Video_View_Start,
    
    (
        SELECT action.value 
        FROM UNNEST(bd.video_p50_watched_actions) as action 
        WHERE action.action_type = 'video_view'
        LIMIT 1
    ) as Video_View_Mid,
    
    (
        SELECT action.value 
        FROM UNNEST(bd.video_p100_watched_actions) as action 
        WHERE action.action_type = 'video_view'
        LIMIT 1
    ) as Video_View_Complete,
    
    (
        SELECT action.value 
        FROM UNNEST(bd.video_thruplay_watched_actions) as action 
        WHERE action.action_type = 'video_view'
        LIMIT 1
    ) as Video_View_Platform,
    
    -- Extract IVC metrics with custom override logic
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.click_on_platform_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'click'
            LIMIT 1
        )
    ) as Click_On_Platform,
    
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.click_to_website_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'link_click'
            LIMIT 1
        )
    ) as Click_To_Website,
    
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.lead_website_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'onsite_web_lead'
            LIMIT 1
        )
    ) as Lead_Website,
    
    -- Lead_Platform - TODO: Figure out what data to map to this
    NULL as Lead_Platform,
    
    (
        SELECT action.value 
        FROM UNNEST(bd.video_p25_watched_actions) as action 
        WHERE action.action_type = 'video_view'
        LIMIT 1
    ) as Video_View_Quarter,
    
    (
        SELECT action.value 
        FROM UNNEST(bd.video_p75_watched_actions) as action 
        WHERE action.action_type = 'video_view'
        LIMIT 1
    ) as Video_View_Three_Quarter,
    
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.video_view_3sec_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'video_view'
            LIMIT 1
        )
    ) as Video_View_3sec,
    
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.view_content_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'view_content'
            LIMIT 1
        )
    ) as View_Content,
    
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.add_to_cart_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'add_to_cart'
            LIMIT 1
        )
    ) as Add_To_Cart,
    
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.initiate_checkout_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'initiate_checkout'
            LIMIT 1
        )
    ) as Initiate_Checkout,
    
    COALESCE(
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = cat.complete_registration_custom_action
            LIMIT 1
        ),
        (
            SELECT action.value 
            FROM UNNEST(bd.actions) as action 
            WHERE action.action_type = 'complete_registration'
            LIMIT 1
        )
    ) as Complete_Registration,
    
    -- Create unique key for merging
    CONCAT(
        COALESCE(bd.client_id, ''), '_',
        COALESCE(bd.ad_id, ''), '_',
        COALESCE(CAST(bd.date_start AS STRING), '')
    ) as merge_key
    
    FROM base_data bd
    LEFT JOIN custom_action_types cat
    ON cat.client_id = bd.client_id
    AND cat.ad_id = bd.ad_id
    AND cat.date_start = bd.date_start
    
    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CONCAT(
        COALESCE(bd.client_id, ''), '_',
        COALESCE(bd.ad_id, ''), '_',
        COALESCE(CAST(bd.date_start AS STRING), '')
    )
    ORDER BY bd.date_start DESC
    ) = 1
)

SELECT * FROM final_rollup
) AS source

ON target.merge_key = source.merge_key

WHEN MATCHED THEN
UPDATE SET
    client_id = source.client_id,
    reporting_group = source.reporting_group,
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
    actions = source.actions,
    video_play_actions = source.video_play_actions,
    video_p50_watched_actions = source.video_p50_watched_actions,
    video_p100_watched_actions = source.video_p100_watched_actions,
    ad_set_contains = source.ad_set_contains,
    Lead = source.Lead,
    Video_View = source.Video_View,
    Purchase = source.Purchase,
    Page_View = source.Page_View,
    Link_Click = source.Link_Click,
    Page_Engagement = source.Page_Engagement,
    Post_Engagement = source.Post_Engagement,
    Landing_Page_View = source.Landing_Page_View,
    Post_Reaction = source.Post_Reaction,
    Post_Save = source.Post_Save,
    Web_Lead = source.Web_Lead,
    Video_View_Start = source.Video_View_Start,
    Video_View_Mid = source.Video_View_Mid,
    Video_View_Complete = source.Video_View_Complete,
    Video_View_Platform = source.Video_View_Platform,
    Click_On_Platform = source.Click_On_Platform,
    Click_To_Website = source.Click_To_Website,
    Lead_Website = source.Lead_Website,
    Lead_Platform = source.Lead_Platform,
    Video_View_Quarter = source.Video_View_Quarter,
    Video_View_Three_Quarter = source.Video_View_Three_Quarter,
    Video_View_3sec = source.Video_View_3sec,
    View_Content = source.View_Content,
    Add_To_Cart = source.Add_To_Cart,
    Initiate_Checkout = source.Initiate_Checkout,
    Complete_Registration = source.Complete_Registration,
    quality_ranking = source.quality_ranking,
    engagement_rate_ranking = source.engagement_rate_ranking,
    conversion_rate_ranking = source.conversion_rate_ranking,
    objective = source.objective,
    optimization_goal = source.optimization_goal

WHEN NOT MATCHED THEN
INSERT (
    client_id, reporting_group, platform, date_start, date_end,
    ad_name, account_id, account_name, account_currency, ad_id, adset_id, adset_name,
    campaign_name, impressions, reach, frequency, spend, clicks, cpc, cpm, cpp, ctr,
    unique_clicks, unique_ctr, cost_per_unique_click, inline_link_clicks, 
    inline_link_click_ctr, actions, video_play_actions, video_p50_watched_actions, 
    video_p100_watched_actions, ad_set_contains, Lead, Video_View, Purchase, Page_View, Link_Click,
    Page_Engagement, Post_Engagement, Landing_Page_View, Post_Reaction, Post_Save,
    Web_Lead, Video_View_Start, Video_View_Mid, Video_View_Complete, Video_View_Platform, 
    quality_ranking, engagement_rate_ranking, conversion_rate_ranking, 
    objective, optimization_goal, merge_key
)
VALUES (
    source.client_id, source.reporting_group, source.platform, source.date_start, source.date_end, source.ad_name,
    source.account_id, source.account_name, source.account_currency, source.ad_id, 
    source.adset_id, source.adset_name, source.campaign_name, source.impressions, 
    source.reach, source.frequency, source.spend, source.clicks, source.cpc, 
    source.cpm, source.cpp, source.ctr, source.unique_clicks, source.unique_ctr, 
    source.cost_per_unique_click,     source.inline_link_clicks, source.inline_link_click_ctr,
    source.actions, source.video_play_actions, source.video_p50_watched_actions, 
    source.video_p100_watched_actions, source.ad_set_contains, source.Lead, source.Video_View, source.Purchase, source.Page_View, source.Link_Click,
    source.Page_Engagement, source.Post_Engagement, source.Landing_Page_View, 
    source.Post_Reaction, source.Post_Save, source.Web_Lead, source.Video_View_Start, 
    source.Video_View_Mid, source.Video_View_Complete, source.Video_View_Platform,
    source.Click_On_Platform, source.Click_To_Website, source.Lead_Website, source.Lead_Platform,
    source.Video_View_Quarter, source.Video_View_Three_Quarter, source.Video_View_3sec,
    source.View_Content, source.Add_To_Cart, source.Initiate_Checkout, source.Complete_Registration,
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