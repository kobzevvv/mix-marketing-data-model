import numpy as np
import pandas as pd
import yaml
from itertools import product

# Load config
with open('mmm_monte_carlo_config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Define inputs from config
saturation_range = np.linspace(config['saturation_power_range']['min'], config['saturation_power_range']['max'], 8)
decay_range = np.linspace(config['decay_range']['min'], config['decay_range']['max'], 10)
scale_factors = config['revenue_scale_factors']
baseline_spend = config['baseline_spend_default']

# Example input: daily untracked revenue for 10 days
untracked_revenue = np.array([1000, 950, 875, 820, 760, 720, 680, 630, 590, 550])
days = np.arange(len(untracked_revenue))

# Store results
results = []

# Monte Carlo grid search
for saturation, decay, scale_factor in product(saturation_range, decay_range, scale_factors):
    # Simulate base contribution (decay x saturation over time)
    base_contribution = (decay ** days) * (baseline_spend ** (1 - saturation))

    # Scale to target revenue
    total_target_revenue = untracked_revenue.sum() * scale_factor
    modeled_contribution = base_contribution * (total_target_revenue / base_contribution.sum())

    # Compute squared deviation loss
    loss = np.sum((untracked_revenue - modeled_contribution) ** 2)

    results.append({
        'saturation': round(saturation, 3),
        'decay': round(decay, 3),
        'scale_factor': round(scale_factor, 2),
        'loss': round(loss, 2)
    })

# Output best 5 combinations
top_fits = pd.DataFrame(results).sort_values('loss').head(5)
print(top_fits)
