with

-- step 1: total daily revenue
daily_total_revenue as (
    select
        date,
        sum(total_revenue) as total_revenue
    from sales
    group by date
),

-- step 2: daily tracked revenue from tracked channels (e.g. Meta, Google)
daily_tracked_revenue as (
    select
        date,
        sum(tracked_revenue) as tracked_revenue
    from tracked_channels
    group by date
),

-- step 3: compute untracked revenue
daily_untracked_revenue as (
    select
        daily_total_revenue.date,
        daily_total_revenue.total_revenue,
        coalesce(daily_tracked_revenue.tracked_revenue, 0) as tracked_revenue,
        daily_total_revenue.total_revenue - coalesce(daily_tracked_revenue.tracked_revenue, 0) as untracked_revenue
    from daily_total_revenue
    left join daily_tracked_revenue using (date)
),

-- step 4: model parameters (from contribution model)
channel_contribution_model_clean as (
    select
        channel,
        region,
        saturation,
        decay,
        model_applied_from_date
    from channel_contribution_model
),

-- step 5: calendar dates for modeling (from min model start to today)
calendar_dates as (
    select
        date
    from (
        select min(model_applied_from_date) as start_date from channel_contribution_model_clean
    ) as date_range
    array join range(start_date, today(), interval 1 day) as date
),

-- step 6: expand model per applicable date
daily_model_application as (
    select
        calendar_dates.date,
        channel_contribution_model_clean.channel,
        channel_contribution_model_clean.region,
        channel_contribution_model_clean.saturation,
        channel_contribution_model_clean.decay,
        channel_contribution_model_clean.model_applied_from_date
    from channel_contribution_model_clean
    join calendar_dates
        on calendar_dates.date >= channel_contribution_model_clean.model_applied_from_date
),

-- step 7: simulate modeled contribution per day
daily_modeled_contribution as (
    select
        date,
        channel,
        region,
        10000 as simulated_spend,
        power(10000, 1 - saturation) as saturated_contribution,
        date_diff('day', model_applied_from_date, date) as days_since_model_start,
        power(decay, days_since_model_start) * saturated_contribution as modeled_contribution
    from daily_model_application
),

-- step 8: sum modeled contribution across all channels and regions per day
daily_total_modeled_revenue as (
    select
        date,
        sum(modeled_contribution) as modeled_untracked_revenue
    from daily_modeled_contribution
    group by date
),

-- step 9: final comparison
daily_model_compliance_check as (
    select
        daily_untracked_revenue.date,
        daily_untracked_revenue.total_revenue,
        daily_untracked_revenue.tracked_revenue,
        daily_untracked_revenue.untracked_revenue,
        daily_total_modeled_revenue.modeled_untracked_revenue,
        daily_untracked_revenue.untracked_revenue - daily_total_modeled_revenue.modeled_untracked_revenue as revenue_difference,
        round(1 - abs(daily_untracked_revenue.untracked_revenue - daily_total_modeled_revenue.modeled_untracked_revenue) / daily_untracked_revenue.untracked_revenue, 3) as relative_accuracy
    from daily_untracked_revenue
    left join daily_total_modeled_revenue using (date)
)

-- final
select * from daily_model_compliance_check
order by date
