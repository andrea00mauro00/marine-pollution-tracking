"""
Data templates and constants for the Marine Pollution Monitoring System.
These values define thresholds, risk factors, and recommendations
used throughout the Flink processing pipeline.
"""

# Pollution thresholds for various measurements
POLLUTION_THRESHOLDS = {
    # Oil content in water (ppm)
    "oil_content": {
        "low": 0.03,
        "medium": 0.1,
        "high": 0.3
    },
    # Turbidity in NTU (Nephelometric Turbidity Units)
    "turbidity": {
        "low": 10,
        "medium": 20,
        "high": 30
    },
    # pH normal range
    "ph": {
        "min_normal": 6.5,
        "max_normal": 8.5
    },
    # Dissolved oxygen in mg/L
    "dissolved_oxygen": {
        "critical": 2.0,  # Hypoxic conditions
        "low": 4.0,       # Stressful for most aquatic life
        "normal": 6.0     # Acceptable for most species
    },
    # Temperature deviation from normal (°C)
    "temperature_delta": {
        "normal": 1.0,
        "concerning": 2.0,
        "high": 3.0
    },
    # Chlorophyll a in μg/L (indicates algal blooms)
    "chlorophyll": {
        "normal": 2.5,
        "elevated": 10.0,
        "bloom": 25.0
    },
    # Wave height in meters (used for dispersion models)
    "wave_height": {
        "low": 0.5,
        "medium": 1.5,
        "high": 3.0
    }
}

# Risk factors for calculating pollution risk score
# Higher values mean the parameter has more impact on overall risk
RISK_FACTORS = {
    "oil_content": 5.0,       # High impact on wildlife and coastal areas
    "turbidity": 1.0,         # Indicates suspended particles
    "ph_deviation": 1.5,      # pH deviation from normal range
    "temperature_delta": 1.2, # Temperature change impact
    "chlorophyll": 0.8,       # Algal bloom indicator
    "dissolved_oxygen": -2.0, # Negative because lower DO is worse
    "salinity_deviation": 0.7 # Salinity change from normal
}

# Recommendations based on pollution level
RECOMMENDATIONS = {
    "high": [
        "Deploy containment booms immediately",
        "Notify coastal authorities and issue public health advisories",
        "Activate emergency response team for cleanup operations",
        "Increase water quality monitoring frequency to hourly intervals",
        "Restrict access to affected areas and nearby beaches",
        "Deploy absorbent materials for oil collection",
        "Conduct wildlife impact assessment",
        "Mobilize specialized cleanup vessels",
        "Implement drone surveillance for continuous monitoring"
    ],
    "medium": [
        "Increase monitoring frequency to every 4 hours",
        "Prepare response equipment and place cleanup teams on standby",
        "Alert local authorities about potential escalation",
        "Conduct additional water sampling at different depths",
        "Begin tracking potential spread using current models",
        "Notify nearby aquaculture and fishing operations",
        "Prepare public information materials if situation escalates",
        "Deploy additional sensor buoys in adjacent areas"
    ],
    "low": [
        "Continue routine monitoring protocols",
        "Document conditions and add to baseline dataset",
        "Review prevention measures and contingency plans",
        "Check sensor calibration and data quality",
        "Update dispersion models with current conditions",
        "Conduct scheduled maintenance on monitoring equipment"
    ]
}

# Pollution type identification criteria
POLLUTION_TYPES = {
    "oil": {
        "indicators": ["surface_sheen", "oil_content", "hydrocarbon_detection"],
        "severity_multiplier": 1.5
    },
    "chemical": {
        "indicators": ["ph_deviation", "conductivity_change", "dissolved_oxygen_drop"],
        "severity_multiplier": 1.8
    },
    "algal_bloom": {
        "indicators": ["chlorophyll", "dissolved_oxygen_fluctuation", "water_discoloration"],
        "severity_multiplier": 1.2
    },
    "sediment": {
        "indicators": ["turbidity", "total_suspended_solids"],
        "severity_multiplier": 0.8
    },
    "thermal": {
        "indicators": ["temperature_delta", "dissolved_oxygen_drop"],
        "severity_multiplier": 1.0
    },
    "plastic": {
        "indicators": ["microplastic_count", "surface_debris"],
        "severity_multiplier": 1.3
    }
}

# Alert levels and notification targets
ALERT_LEVELS = {
    "critical": {
        "notification_channels": ["email", "sms", "api_webhook"],
        "check_interval_minutes": 15,
        "expiry_hours": 48
    },
    "high": {
        "notification_channels": ["email", "api_webhook"],
        "check_interval_minutes": 30,
        "expiry_hours": 36
    },
    "medium": {
        "notification_channels": ["email"],
        "check_interval_minutes": 60,
        "expiry_hours": 24
    },
    "low": {
        "notification_channels": ["dashboard_only"],
        "check_interval_minutes": 240,
        "expiry_hours": 12
    }
}