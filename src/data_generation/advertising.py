import pandas as pd
import numpy as np
import random
import datetime
from faker import Faker
import os
import uuid

# Set up Faker with locale support - correction du locale non supporté fr_SN
fake = Faker(['fr_FR'])  # Utilisation seulement de fr_FR au lieu de fr_SN
Faker.seed(42)  # For reproducibility

# Ajout des noms et prénoms sénégalais
senegalese_first_names = [
    "Mamadou", "Abdoulaye", "Ousmane", "Modou", "Ibrahima", "Cheikh", "Moussa", "Assane",
    "Pape", "Idrissa", "Alioune", "Mbaye", "Samba", "Babacar", "Seydou", "Omar", "Aliou",
    "Gora", "Demba", "Boubacar", "Maguette", "Serigne", "Malick", "Daouda", "Amadou",
    "Lamine", "Pathé", "Souleymane", "Youssou", "Ismaïla", "Mouhamed", "Tidiane", "Bocar",
    "Mor", "Abdoul", "Bassirou", "Ababacar", "Landing", "Thierno", "Bara", "Mamour",
    "Massamba", "El Hadji", "Diaga", "Saliou", "Ilyass", "Karim", "Moustapha", "Ndiaga",
    "Fadel", "Birame", "Mansour", "Doudou", "Boucounta", "Gorgui", "Habib", "Ablaye",
    "Sidy", "Bamba", "Saër", "Madické", "Saïdou", "Malal", "Falilou", "Khadim", "Malamine",
    "Fatou", "Aminata", "Aïssatou", "Rokhaya", "Mariama", "Awa", "Khady", "Dieynaba",
    "Sokhna", "Ndeye", "Astou", "Fatoumata", "Rama", "Mame", "Adja", "Sophie", "Coumba",
    "Nabou", "Soda", "Bineta", "Yacine", "Bintou", "Fama", "Ramatoulaye", "Safiétou",
    "Dior", "Yaye", "Tening", "Mbayang", "Penda", "Maty", "Kiné", "Seynabou", "Fari",
    "Adjara", "Salimata", "Marième", "Anta", "Saly", "Oumy", "Marème", "Tida", "Diarra",
    "Ndèye", "Diouma", "Magatte", "Ndella", "Kadija", "Maïmouna", "Tata", "Ndioro",
    "Yandé", "Diama", "Codou", "Bérénice", "Aissatou", "Amy", "Ngoné", "Mbathio"
]

senegalese_last_names = [
    "Diop", "Ndiaye", "Fall", "Gueye", "Seck", "Mbaye", "Diouf", "Diallo", "Cissé",
    "Ndao", "Faye", "Sarr", "Thiam", "Sow", "Sy", "Ba", "Ka", "Niang", "Bâ", "Lô",
    "Diagne", "Kane", "Wade", "Samb", "Beye", "Mendy", "Camara", "Sène", "Badji",
    "Ndoye", "Thiaw", "Mboup", "Diatta", "Ndour", "Sall", "Diakhaté", "Mbodj", "Ndir",
    "Dione", "Toure", "Gomis", "Goudiaby", "Sané", "Bassène", "Bakhoum", "Coly",
    "Gning", "Tine", "Diarra", "Sylla", "Konaté", "Sonko", "Niasse", "Dramé",
    "Diedhiou", "Kébé", "Kaïré", "Fofana", "Kourouma", "Doucouré", "Tandian",
    "Sagna", "Baïla", "Bousso", "Ngom", "Sarr", "Dabo", "Sakho", "Fadiga", "Boye",
    "Nguirane", "Diassy", "Koné", "Tounkara", "Bathily", "Coulibaly", "Touré",
    "Sow", "Bocar", "Barry", "Khouma"
]

# Fonction pour générer un nom sénégalais
def generate_senegalese_name():
    first_name = random.choice(senegalese_first_names)
    last_name = random.choice(senegalese_last_names)
    return f"{first_name} {last_name}"

# Create output directory if it doesn't exist
base_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(base_dir, '..', '..', 'data', 'raw', 'advertising')
os.makedirs(output_dir, exist_ok=True)

# Date range for data generation
start_date = datetime.datetime(2024, 1, 1)
end_date = datetime.datetime(2025, 3, 15)  # Current date in the scenario

# Define ad platforms with their characteristics
platforms = {
    "Facebook": {
        "ad_types": ["Image", "Carousel", "Video"],
        "avg_cpc": {"Image": 200, "Carousel": 250, "Video": 300},  # CPC in CFA francs
        "ctr_range": {"Image": (0.01, 0.03), "Carousel": (0.015, 0.04), "Video": (0.02, 0.05)},
        "conversion_rate_range": {"Image": (0.01, 0.025), "Carousel": (0.015, 0.03), "Video": (0.02, 0.04)},
        "budget_weight": 0.35  # Portion of total budget
    },
    "Instagram": {
        "ad_types": ["Image", "Story", "Reel"],
        "avg_cpc": {"Image": 300, "Story": 350, "Reel": 400},
        "ctr_range": {"Image": (0.02, 0.04), "Story": (0.03, 0.06), "Reel": (0.035, 0.07)},
        "conversion_rate_range": {"Image": (0.015, 0.03), "Story": (0.02, 0.04), "Reel": (0.025, 0.05)},
        "budget_weight": 0.3
    },
    "Google": {
        "ad_types": ["Search", "Display", "Shopping"],
        "avg_cpc": {"Search": 350, "Display": 150, "Shopping": 400},
        "ctr_range": {"Search": (0.03, 0.08), "Display": (0.005, 0.015), "Shopping": (0.02, 0.05)},
        "conversion_rate_range": {"Search": (0.03, 0.06), "Display": (0.005, 0.015), "Shopping": (0.025, 0.05)},
        "budget_weight": 0.2
    },
    "WhatsApp": {  # Business API ads
        "ad_types": ["Click-to-WhatsApp"],
        "avg_cpc": {"Click-to-WhatsApp": 150},
        "ctr_range": {"Click-to-WhatsApp": (0.03, 0.06)},
        "conversion_rate_range": {"Click-to-WhatsApp": (0.02, 0.05)},
        "budget_weight": 0.1
    },
    "YouTube": {
        "ad_types": ["TrueView", "Bumper"],
        "avg_cpc": {"TrueView": 250, "Bumper": 200},
        "ctr_range": {"TrueView": (0.01, 0.025), "Bumper": (0.005, 0.015)},
        "conversion_rate_range": {"TrueView": (0.01, 0.025), "Bumper": (0.005, 0.015)},
        "budget_weight": 0.05
    }
}

# Campaign themes relevant to Senegalese cosmetics market
campaign_themes = [
    "Beauté Naturelle",
    "Peau Sans Tache",
    "Soins Capillaires",
    "Produits Bio",
    "Promo Ramadan",
    "Spécial Tabaski",
    "Teint Éclatant",
    "Ingrédients Africains",
    "Tendance Maquillage",
    "Coffrets Cadeaux"
]

# Audience demographics for Senegal
audiences = [
    "Femmes 18-24",
    "Femmes 25-34",
    "Femmes 35-44",
    "Hommes 25-34",
    "Urbain Dakar",
    "Villes secondaires",
    "Étudiants",
    "Professionnels",
    "Intérêt beauté",
    "Intérêt mode"
]

# Product categories
categories = ["soin_visage", "soin_corps", "soin_cheveux", "maquillage", "parfum"]

# Generate Google Ads data
def generate_google_ads(start_date, end_date, num_campaigns=50):
    ads_data = []
    
    # Budget distribution throughout the year with peaks during holidays
    def get_budget_multiplier(date):
        month = date.month
        day = date.day
        
        # Ramadan (approximate - varies yearly)
        if month == 4:
            return 1.5
        # Tabaski (approximate - varies yearly)
        elif month == 9 and day > 15:
            return 1.4
        # End of year holidays
        elif month == 12:
            return 1.6
        # Sales periods
        elif month == 2 or month == 7:
            return 1.3
        # Regular periods
        else:
            return 1.0
    
    # Keywords related to cosmetics in Senegal (French language)
    keywords = [
        "produits cosmétiques naturels",
        "soins peau noire",
        "crème anti-tache",
        "beurre de karité pur",
        "huile de baobab bio",
        "produits éclaircissants naturels",
        "soins cheveux crépus",
        "cosmétiques bio Dakar",
        "maquillage peau foncée",
        "parfum sans alcool",
        "savon noir africain",
        "traitement acné peau noire",
        "cosmétiques Sénégal",
        "huile de coco cheveux",
        "masque visage naturel"
    ]
    
    # Create campaigns
    for i in range(num_campaigns):
        campaign_id = f"GAD-{i+1000}"
        campaign_name = f"{random.choice(campaign_themes)} - {random.choice(categories)}"
        
        # Campaign start and end dates
        campaign_start = fake.date_time_between(start_date=start_date, end_date=end_date - datetime.timedelta(days=7))
        campaign_duration = random.randint(7, 90)  # Between 1 week and 3 months
        campaign_end = min(campaign_start + datetime.timedelta(days=campaign_duration), end_date)
        
        # Basic campaign settings
        daily_budget = random.randint(5000, 25000)  # In CFA francs (approximately $8-$40)
        
        # Generate daily data for this campaign
        current_date = campaign_start
        while current_date <= campaign_end:
            # Adjust budget based on season
            budget_multiplier = get_budget_multiplier(current_date)
            daily_budget_adjusted = int(daily_budget * budget_multiplier)
            
            # Select ad type for this day
            ad_type = random.choice(platforms["Google"]["ad_types"])
            
            # Calculate performance metrics
            impressions = int(daily_budget_adjusted / platforms["Google"]["avg_cpc"][ad_type] * random.uniform(10, 30))
            ctr_min, ctr_max = platforms["Google"]["ctr_range"][ad_type]
            ctr = random.uniform(ctr_min, ctr_max)
            clicks = int(impressions * ctr)
            
            # Sometimes there are days with technical issues or paused campaigns
            if random.random() < 0.05:  # 5% chance of abnormal day
                impressions = int(impressions * random.uniform(0.1, 0.5))
                clicks = int(impressions * ctr)
            
            # Cost and conversions
            avg_cpc = platforms["Google"]["avg_cpc"][ad_type] * random.uniform(0.8, 1.2)  # Some variation
            cost = clicks * avg_cpc
            
            # Conversion metrics
            conv_rate_min, conv_rate_max = platforms["Google"]["conversion_rate_range"][ad_type]
            conversion_rate = random.uniform(conv_rate_min, conv_rate_max)
            conversions = int(clicks * conversion_rate)
            avg_order_value = random.uniform(8000, 20000)  # In CFA francs
            conversion_value = conversions * avg_order_value
            
            # Select keywords used for this day (3-5 keywords)
            daily_keywords = random.sample(keywords, random.randint(3, 5))
            
            # Create record for this day
            record = {
                "date": current_date.strftime("%Y-%m-%d"),
                "platform": "Google",
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "ad_type": ad_type,
                "keywords": "|".join(daily_keywords),
                "impressions": impressions,
                "clicks": clicks,
                "ctr": ctr,
                "avg_cpc": avg_cpc,
                "cost": cost,
                "conversions": conversions,
                "conversion_rate": conversion_rate,
                "conversion_value": conversion_value,
                "target_audience": random.choice(audiences),
                "category": random.choice(categories)
            }
            
            ads_data.append(record)
            
            # Move to next day
            current_date += datetime.timedelta(days=1)
    
    return ads_data

# Generate social media ads data (Facebook, Instagram, etc.)
def generate_social_media_ads(start_date, end_date, num_campaigns=80):
    ads_data = []
    
    # Social media platform weights
    platform_weights = {
        "Facebook": 0.4,
        "Instagram": 0.35,
        "WhatsApp": 0.15,
        "YouTube": 0.1
    }
    
    # Content themes for ads
    content_themes = [
        "Témoignage client",
        "Avant/Après",
        "Tutoriel beauté",
        "Ingrédients naturels",
        "Promotion produit",
        "Tendance beauté",
        "Conseils d'experts",
        "Style de vie",
        "Événement spécial",
        "Collaboration influenceur"
    ]
    
    # Create campaigns across different platforms
    for i in range(num_campaigns):
        # Select platform
        platform = random.choices(
            list(platform_weights.keys()),
            weights=list(platform_weights.values())
        )[0]
        
        campaign_id = f"{platform[:2].upper()}-{i+2000}"
        
        # Campaign theme and creative
        theme = random.choice(campaign_themes)
        content = random.choice(content_themes)
        campaign_name = f"{theme} - {content}"
        
        # Campaign dates
        campaign_start = fake.date_time_between(start_date=start_date, end_date=end_date - datetime.timedelta(days=7))
        campaign_duration = random.randint(7, 60)  # Between 1 week and 2 months
        campaign_end = min(campaign_start + datetime.timedelta(days=campaign_duration), end_date)
        
        # Basic campaign settings
        daily_budget = random.randint(3000, 20000)  # In CFA francs
        
        # Target audience
        audience = random.choice(audiences)
        
        # Generate daily data
        current_date = campaign_start
        while current_date <= campaign_end:
            # Weekend vs weekday effect
            is_weekend = current_date.weekday() >= 5  # 5=Saturday, 6=Sunday
            weekend_multiplier = 0.8 if is_weekend else 1.1  # Lower engagement on weekends in some markets
            
            # Select ad type for this day
            ad_type = random.choice(platforms[platform]["ad_types"])
            
            # Performance metrics with more realistic patterns
            impressions_base = int(daily_budget / platforms[platform]["avg_cpc"][ad_type] * random.uniform(15, 40))
            
            # Campaign fatigue effect (performance drops over time)
            days_running = (current_date - campaign_start).days + 1
            fatigue_factor = max(0.7, 1 - (days_running / campaign_duration) * 0.4)
            
            # Apply various factors
            impressions = int(impressions_base * weekend_multiplier * fatigue_factor)
            
            # CTR with natural variation
            ctr_min, ctr_max = platforms[platform]["ctr_range"][ad_type]
            
            # More variation based on time factors
            hour_variation = random.uniform(0.9, 1.1)  # Time of day effect
            ctr_base = random.uniform(ctr_min, ctr_max)
            ctr = ctr_base * weekend_multiplier * fatigue_factor * hour_variation
            
            # Derived metrics
            clicks = int(impressions * ctr)
            
            # Cost calculation
            avg_cpc = platforms[platform]["avg_cpc"][ad_type] * random.uniform(0.85, 1.15)
            cost = clicks * avg_cpc
            
            # Engagement metrics specific to social media
            engagement_rate = ctr * random.uniform(1.5, 3)  # Higher than CTR
            engagements = int(impressions * engagement_rate)
            
            likes = int(engagements * random.uniform(0.4, 0.6))
            comments = int(engagements * random.uniform(0.05, 0.15))
            shares = int(engagements * random.uniform(0.01, 0.1))
            
            # Conversion metrics
            conv_rate_min, conv_rate_max = platforms[platform]["conversion_rate_range"][ad_type]
            conversion_rate = random.uniform(conv_rate_min, conv_rate_max) * fatigue_factor
            conversions = int(clicks * conversion_rate)
            avg_order_value = random.uniform(7500, 18000)
            conversion_value = conversions * avg_order_value
            
            # Create record
            record = {
                "date": current_date.strftime("%Y-%m-%d"),
                "platform": platform,
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "ad_type": ad_type,
                "content_theme": content,
                "impressions": impressions,
                "clicks": clicks,
                "ctr": ctr,
                "avg_cpc": avg_cpc,
                "cost": cost,
                "engagements": engagements,
                "likes": likes,
                "comments": comments,
                "shares": shares,
                "conversions": conversions,
                "conversion_rate": conversion_rate,
                "conversion_value": conversion_value,
                "target_audience": audience,
                "category": random.choice(categories)
            }
            
            ads_data.append(record)
            
            # Move to next day
            current_date += datetime.timedelta(days=1)
    
    return ads_data

# Generate influencer marketing data
def generate_influencer_data(start_date, end_date, num_influencers=30):
    influencer_data = []
    
    # Influencer tiers
    tiers = {
        "Micro": {"followers_range": (5000, 20000), "fee_range": (50000, 200000), "engagement_range": (0.03, 0.06)},
        "Mid": {"followers_range": (20001, 100000), "fee_range": (200001, 500000), "engagement_range": (0.02, 0.045)},
        "Macro": {"followers_range": (100001, 500000), "fee_range": (500001, 1500000), "engagement_range": (0.015, 0.03)}
    }
    
    # Social platforms for influencers
    platforms = ["Instagram", "TikTok", "YouTube", "Facebook"]
    
    # Common Senegalese influencer niches
    niches = ["Beauté", "Mode", "Style de vie", "Bien-être", "Cuisine", "Voyage"]
    
    # Influencer profiles
    influencers = []
    for i in range(num_influencers):
        # Select tier
        tier = random.choices(list(tiers.keys()), weights=[0.6, 0.3, 0.1])[0]
        
        # Basic profile
        followers_min, followers_max = tiers[tier]["followers_range"]
        followers = random.randint(followers_min, followers_max)
        
        fee_min, fee_max = tiers[tier]["fee_range"]
        base_fee = random.randint(fee_min, fee_max)
        
        engagement_min, engagement_max = tiers[tier]["engagement_range"]
        avg_engagement = random.uniform(engagement_min, engagement_max)
        
        # Platform and niche
        primary_platform = random.choice(platforms)
        niche = random.choice(niches)
        
        # Region of influence (Senegal specific)
        if random.random() < 0.6:  # 60% Dakar-based
            region = "Dakar"
        else:
            region = random.choice(["Thiès", "Saint-Louis", "Touba", "Diaspora"])
        
        # Language used
        language = random.choices(["Français", "Wolof", "Français/Wolof"], weights=[0.3, 0.2, 0.5])[0]
        
        influencer = {
            "influencer_id": f"INF-{i+100}",
            "name": generate_senegalese_name(),  # Utilisation de noms sénégalais
            "tier": tier,
            "followers": followers,
            "primary_platform": primary_platform,
            "niche": niche,
            "region": region,
            "language": language,
            "base_fee": base_fee,
            "avg_engagement_rate": avg_engagement
        }
        
        influencers.append(influencer)
    
    # Generate campaigns with influencers
    campaigns = []
    
    for i in range(num_influencers * 2):  # Each influencer does about 2 campaigns on average
        campaign_id = f"INF-CAMP-{i+500}"
        campaign_name = f"{random.choice(campaign_themes)} - {random.choice(['Lancement', 'Promotion', 'Branding'])}"
        
        # Campaign dates
        campaign_start = fake.date_time_between(start_date=start_date, end_date=end_date - datetime.timedelta(days=30))
        campaign_duration = random.randint(7, 30)  # 1 week to 1 month
        campaign_end = min(campaign_start + datetime.timedelta(days=campaign_duration), end_date)
        
        # Select random influencer
        influencer = random.choice(influencers)
        
        # Content types
        content_types = {
            "Instagram": ["Post", "Story", "Reel"],
            "TikTok": ["Video"],
            "YouTube": ["Video", "Short"],
            "Facebook": ["Post", "Live"]
        }
        
        # Products featured
        products_featured = random.sample([
            "Sérum au Karité",
            "Crème au Baobab",
            "Huile de Ricin",
            "Beurre de Karité",
            "Masque à l'Argile",
            "Fond de Teint Naturel",
            "Parfum à l'Hibiscus"
        ], random.randint(1, 3))
        
        # Promo code
        promo_code = f"{influencer['name'].split()[0].upper()}{random.randint(10, 30)}"
        
        # Performance metrics
        content_type = random.choice(content_types[influencer["primary_platform"]])
        
        # Base metrics
        impressions = int(influencer["followers"] * random.uniform(0.6, 1.2))  # Reach can be more or less than followers
        engagement_rate = influencer["avg_engagement_rate"] * random.uniform(0.8, 1.2)  # Variation
        engagements = int(impressions * engagement_rate)
        
        # Click metrics
        ctr = random.uniform(0.01, 0.05)  # Click-through rate to e-commerce
        clicks = int(impressions * ctr)
        
        # Conversion metrics
        conversion_rate = random.uniform(0.01, 0.04)
        conversions = int(clicks * conversion_rate)
        avg_order_value = random.uniform(8000, 15000)
        conversion_value = conversions * avg_order_value
        
        # ROI calculation
        cost = influencer["base_fee"] * random.uniform(0.9, 1.1)  # Some negotiation
        roi = (conversion_value - cost) / cost if cost > 0 else 0
        
        campaign = {
            "campaign_id": campaign_id,
            "campaign_name": campaign_name,
            "start_date": campaign_start.strftime("%Y-%m-%d"),
            "end_date": campaign_end.strftime("%Y-%m-%d"),
            "influencer_id": influencer["influencer_id"],
            "influencer_name": influencer["name"],
            "platform": influencer["primary_platform"],
            "content_type": content_type,
            "products_featured": "|".join(products_featured),
            "promo_code": promo_code,
            "impressions": impressions,
            "engagements": engagements,
            "engagement_rate": engagement_rate,
            "clicks": clicks,
            "ctr": ctr,
            "conversions": conversions,
            "conversion_rate": conversion_rate,
            "conversion_value": conversion_value,
            "cost": cost,
            "roi": roi,
            "category": random.choice(categories)
        }
        
        campaigns.append(campaign)
    
    return influencers, campaigns

# Generate all advertising data
print("Generating Google Ads data...")
google_ads_data = generate_google_ads(start_date, end_date, num_campaigns=50)
google_ads_df = pd.DataFrame(google_ads_data)

print("Generating social media ads data...")
social_ads_data = generate_social_media_ads(start_date, end_date, num_campaigns=80)
social_ads_df = pd.DataFrame(social_ads_data)

print("Generating influencer marketing data...")
influencers_data, campaigns_data = generate_influencer_data(start_date, end_date, num_influencers=30)
influencers_df = pd.DataFrame(influencers_data)
campaigns_df = pd.DataFrame(campaigns_data)

# Save all data
google_ads_df.to_csv(os.path.join(output_dir, 'google_ads.csv'), index=False)
social_ads_df.to_csv(os.path.join(output_dir, 'social_ads.csv'), index=False)
influencers_df.to_csv(os.path.join(output_dir, 'influencers.csv'), index=False)
campaigns_df.to_csv(os.path.join(output_dir, 'influencer_campaigns.csv'), index=False)

# Combine all platform data for an integrated view
all_platform_data = []

# Process Google Ads data
for _, row in google_ads_df.iterrows():
    all_platform_data.append({
        "date": row["date"],
        "platform": row["platform"],
        "campaign_id": row["campaign_id"],
        "campaign_name": row["campaign_name"],
        "ad_type": row["ad_type"],
        "impressions": row["impressions"],
        "clicks": row["clicks"],
        "cost": row["cost"],
        "conversions": row["conversions"],
        "conversion_value": row["conversion_value"],
        "target_audience": row["target_audience"],
        "category": row["category"]
    })

# Process Social Media Ads data
for _, row in social_ads_df.iterrows():
    all_platform_data.append({
        "date": row["date"],
        "platform": row["platform"],
        "campaign_id": row["campaign_id"],
        "campaign_name": row["campaign_name"],
        "ad_type": row["ad_type"],
        "impressions": row["impressions"],
        "clicks": row["clicks"],
        "cost": row["cost"],
        "conversions": row["conversions"],
        "conversion_value": row["conversion_value"],
        "target_audience": row["target_audience"],
        "category": row["category"] if "category" in row else None
    })

# Process Influencer data (aggregated to daily level)
for _, campaign in campaigns_df.iterrows():
    # Convert campaign period to daily entries
    start = datetime.datetime.strptime(campaign["start_date"], "%Y-%m-%d")
    end = datetime.datetime.strptime(campaign["end_date"], "%Y-%m-%d")
    
    # Calculate daily metrics (simplified by dividing evenly across days)
    days = (end - start).days + 1
    
    # Only add if days is positive
    if days > 0:
        daily_impressions = int(campaign["impressions"] / days)
        daily_clicks = int(campaign["clicks"] / days)
        daily_cost = campaign["cost"] / days
        daily_conversions = int(campaign["conversions"] / days)
        daily_conversion_value = campaign["conversion_value"] / days
        
        current_date = start
        while current_date <= end:
            all_platform_data.append({
                "date": current_date.strftime("%Y-%m-%d"),
                "platform": "Influencer_" + campaign["platform"],
                "campaign_id": campaign["campaign_id"],
                "campaign_name": campaign["campaign_name"],
                "ad_type": campaign["content_type"],
                "impressions": daily_impressions,
                "clicks": daily_clicks,
                "cost": daily_cost,
                "conversions": daily_conversions,
                "conversion_value": daily_conversion_value,
                "target_audience": "Followers",
                "category": campaign["category"] if "category" in campaign else None
            })
            
            current_date += datetime.timedelta(days=1)

# Save combined platform data
all_platforms_df = pd.DataFrame(all_platform_data)
all_platforms_df.to_csv(os.path.join(output_dir, 'all_advertising_platforms.csv'), index=False)

print("\nAll advertising data has been generated and saved to CSV files:")
print(f"  - Google Ads: {len(google_ads_df)} entries")
print(f"  - Social Media Ads: {len(social_ads_df)} entries")
print(f"  - Influencers: {len(influencers_df)} influencers")
print(f"  - Influencer Campaigns: {len(campaigns_df)} campaigns")
print(f"  - Combined Platform Data: {len(all_platforms_df)} entries")

# Show a sample of the combined data
print("\nSample of combined advertising data:")
print(all_platforms_df.head())