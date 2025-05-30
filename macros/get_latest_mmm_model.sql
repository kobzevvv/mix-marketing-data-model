{% macro get_latest_mmm_model(pacing_source, model_source) %}
with pacing as (
    select
        Channel,
        Regions as Region,
        Baseline_Spend
    from {{ pacing_source }}
),

mmm as (
    select
        Channel,
        Regions as Region,
        Baseline_Spend,
        `Baseline Estimated Revenue` as Estimated_Revenue,
        `Saturation Power Law x^(1-s)` as Saturation,
        `Decay \nnext day = day * c ` as Decay,
        `Model Applied from date` as Model_Date
    from {{ model_source }}
),

latest_models as (
    select
        p.Channel,
        p.Region,
        -- Choose the latest specific model
        (
            select as struct *
            from mmm m1
            where m1.Channel = p.Channel and m1.Region = p.Region
            order by Model_Date desc
            limit 1
        ) as specific_model,

        -- Choose the latest "All" region model
        (
            select as struct *
            from mmm m2
            where m2.Channel = p.Channel and m2.Region = 'All'
            order by Model_Date desc
            limit 1
        ) as all_model,

        p.Baseline_Spend as pacing_spend
    from pacing p
),

final as (
    select
        Channel,
        Region,
        case
            when specific_model.Estimated_Revenue is not null and all_model.Estimated_Revenue is not null then
                -- Weighted average
                (specific_model.Estimated_Revenue * specific_model.Baseline_Spend + all_model.Estimated_Revenue * all_model.Baseline_Spend)
                / (specific_model.Baseline_Spend + all_model.Baseline_Spend)
            when specific_model.Estimated_Revenue is not null then specific_model.Estimated_Revenue
            else all_model.Estimated_Revenue
        end as Estimated_Revenue,

        case
            when specific_model.Saturation is not null and all_model.Saturation is not null then
                (specific_model.Saturation * specific_model.Baseline_Spend + all_model.Saturation * all_model.Baseline_Spend)
                / (specific_model.Baseline_Spend + all_model.Baseline_Spend)
            when specific_model.Saturation is not null then specific_model.Saturation
            else all_model.Saturation
        end as Saturation,

        case
            when specific_model.Decay is not null and all_model.Decay is not null then
                (specific_model.Decay * specific_model.Baseline_Spend + all_model.Decay * all_model.Baseline_Spend)
                / (specific_model.Baseline_Spend + all_model.Baseline_Spend)
            when specific_model.Decay is not null then specific_model.Decay
            else all_model.Decay
        end as Decay,

        coalesce(specific_model.Model_Date, all_model.Model_Date) as Model_Date
    from latest_models
)

select * from final
{% endmacro %}
