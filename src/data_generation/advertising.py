import pandas as pd
import numpy as np
import random
import datetime
from faker import Faker
import os
import uuid
import argparse
from tqdm import tqdm
import glob

# Set up Faker with locale support
fake = Faker(['fr_FR'])
Faker.seed(42)  # For reproducibility

# Create output directory if it doesn't exist
base_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(base_dir, '..', '..', 'data', 'raw', 'advertising')
os.makedirs(output_dir, exist_ok=True)

# Senegalese names for personalization
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

class AdvertisingDataGenerator:
    def __init__(self, start_date=None, end_date=None, snapshot_frequency='daily'):
        self.start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d') if start_date else datetime.datetime(2025, 1, 1)
        self.end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.datetime(2025, 3, 28)
        
        # Set frequency for snapshot generation
        if snapshot_frequency == 'daily':
            self.freq = 'D'
        elif snapshot_frequency == 'weekly':
            self.freq = 'W'
        elif snapshot_frequency == 'monthly':
            self.freq = 'M'
        else:
            self.freq = 'D'  # Default to daily
            
        # Store existing campaigns
        self.active_campaigns = {
            "google": [],
            "social": [],
            "influencer": []
        }
        
        # Campaign ID registry for other scripts
        self.campaign_id_registry = os.path.join(output_dir, 'campaign_ids.txt')
        
        # Store customer IDs for alignment with CRM
        self.customer_ids = self.load_customer_ids()
        
        # Track current simulation date
        self.current_date = self.start_date
        
    def load_customer_ids(self):
        """Load customer IDs from CRM for alignment"""
        crm_registry = os.path.join(base_dir, '..', '..', 'data', 'raw', 'crm', 'customer_ids.txt')
        customer_ids = []
        
        if os.path.exists(crm_registry):
            with open(crm_registry, 'r') as f:
                customer_ids = [line.strip() for line in f.readlines()]
        
        print(f"Loaded {len(customer_ids)} customer IDs for alignment")
        return customer_ids
    
    def initialize_campaigns(self):
        """Initialize campaigns at the start date"""
        print("Initializing advertising campaigns...")
        
        # Generate initial campaigns with various start/end dates
        # Some campaigns are already active, others will start in the future
        
        # Google Ads campaigns (10 initial)
        google_campaigns = self.generate_google_campaigns(10, self.start_date)
        self.active_campaigns["google"] = google_campaigns
        
        # Social media campaigns (15 initial)
        social_campaigns = self.generate_social_campaigns(15, self.start_date)
        self.active_campaigns["social"] = social_campaigns
        
        # Influencer campaigns (8 initial)
        influencer_campaigns = self.generate_influencer_campaigns(8, self.start_date)
        self.active_campaigns["influencer"] = influencer_campaigns
        
        # Save initial campaign IDs to registry
        self.update_campaign_registry()
        
        print(f"Initialized {len(google_campaigns)} Google campaigns, {len(social_campaigns)} social campaigns, and {len(influencer_campaigns)} influencer campaigns")
    
    def update_campaign_registry(self):
        """Update the campaign ID registry file"""
        with open(self.campaign_id_registry, 'w') as f:
            for platform, campaigns in self.active_campaigns.items():
                for campaign in campaigns:
                    f.write(f"{campaign['campaign_id']}\n")
    
    def generate_google_campaigns(self, num_campaigns, start_date):
        """Generate Google Ads campaigns with various start/end dates"""
        campaigns = []
        
        for i in range(num_campaigns):
            # Campaign basics
            campaign_id = f"GAD-{i+1000}"
            campaign_theme = random.choice(campaign_themes)
            campaign_category = random.choice(categories)
            campaign_name = f"{campaign_theme} - {campaign_category}"
            
            # Campaign duration (between 2 weeks and 3 months)
            duration = random.randint(14, 90)
            
            # Some campaigns start before, some after the simulation start
            days_offset = random.randint(-30, 60)  # -30 to +60 days from start
            campaign_start = start_date + datetime.timedelta(days=days_offset)
            campaign_end = campaign_start + datetime.timedelta(days=duration)
            
            # Campaign budget (higher for seasonal campaigns)
            is_seasonal = campaign_theme in ["Promo Ramadan", "Spécial Tabaski", "Coffrets Cadeaux"]
            daily_budget = random.randint(8000, 25000) * (1.5 if is_seasonal else 1.0)
            
            # Campaign targeting
            target_audience = random.choice(audiences)
            
            campaign = {
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "platform": "Google",
                "start_date": campaign_start,
                "end_date": campaign_end,
                "daily_budget": daily_budget,
                "target_audience": target_audience,
                "category": campaign_category,
                "performance_curve": self.generate_performance_curve(duration)
            }
            
            campaigns.append(campaign)
        
        return campaigns
    
    def generate_social_campaigns(self, num_campaigns, start_date):
        """Generate social media campaigns"""
        campaigns = []
        
        platforms = ["Facebook", "Instagram", "WhatsApp", "TikTok"]
        platform_weights = [0.4, 0.3, 0.2, 0.1]
        
        for i in range(num_campaigns):
            # Platform selection
            platform = random.choices(platforms, weights=platform_weights)[0]
            
            # Campaign basics
            campaign_id = f"{platform[:2].upper()}-{i+2000}"
            campaign_theme = random.choice(campaign_themes)
            campaign_category = random.choice(categories)
            campaign_name = f"{campaign_theme} - {campaign_category}"
            
            # Campaign duration (social campaigns are typically shorter)
            duration = random.randint(7, 60)
            
            # Staggered campaign starts
            days_offset = random.randint(-20, 70)
            campaign_start = start_date + datetime.timedelta(days=days_offset)
            campaign_end = campaign_start + datetime.timedelta(days=duration)
            
            # Campaign budget varies by platform
            if platform == "Facebook":
                daily_budget = random.randint(5000, 20000)
            elif platform == "Instagram":
                daily_budget = random.randint(6000, 22000)
            elif platform == "TikTok":
                daily_budget = random.randint(8000, 25000)
            else:  # WhatsApp
                daily_budget = random.randint(3000, 12000)
            
            # Campaign targeting
            target_audience = random.choice(audiences)
            
            campaign = {
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "platform": platform,
                "start_date": campaign_start,
                "end_date": campaign_end,
                "daily_budget": daily_budget,
                "target_audience": target_audience,
                "category": campaign_category,
                "content_theme": random.choice(["Témoignage", "Tutoriel", "Promotionnel", "Lifestyle"]),
                "performance_curve": self.generate_performance_curve(duration)
            }
            
            campaigns.append(campaign)
        
        return campaigns
    
    def generate_influencer_campaigns(self, num_campaigns, start_date):
        """Generate influencer marketing campaigns"""
        campaigns = []
        
        # First generate influencers
        influencers = []
        for i in range(15):  # Pool of 15 influencers
            tier = random.choices(["Micro", "Mid", "Macro"], weights=[0.6, 0.3, 0.1])[0]
            
            if tier == "Micro":
                followers = random.randint(5000, 20000)
                engagement_rate = random.uniform(0.03, 0.06)
                base_fee = random.randint(50000, 200000)
            elif tier == "Mid":
                followers = random.randint(20001, 100000)
                engagement_rate = random.uniform(0.02, 0.045)
                base_fee = random.randint(200001, 500000)
            else:  # Macro
                followers = random.randint(100001, 500000)
                engagement_rate = random.uniform(0.015, 0.03)
                base_fee = random.randint(500001, 1500000)
                
            influencer = {
                "influencer_id": f"INF-{i+100}",
                "name": self.generate_senegalese_name(),
                "tier": tier,
                "followers": followers,
                "platform": random.choice(["Instagram", "TikTok", "YouTube"]),
                "engagement_rate": engagement_rate,
                "base_fee": base_fee
            }
            
            influencers.append(influencer)
        
        # Then generate campaigns using these influencers
        for i in range(num_campaigns):
            # Select an influencer
            influencer = random.choice(influencers)
            
            # Campaign basics
            campaign_id = f"INF-CAMP-{i+500}"
            campaign_theme = random.choice(campaign_themes)
            campaign_category = random.choice(categories)
            campaign_name = f"{campaign_theme} - Influencer"
            
            # Campaign duration (typically short for influencers)
            duration = random.randint(7, 30)
            
            # Staggered campaign starts
            days_offset = random.randint(-10, 80)
            campaign_start = start_date + datetime.timedelta(days=days_offset)
            campaign_end = campaign_start + datetime.timedelta(days=duration)
            
            # Promo code
            promo_code = f"{influencer['name'].split()[0].upper()}{random.randint(10, 30)}"
            
            campaign = {
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "platform": f"Influencer_{influencer['platform']}",
                "start_date": campaign_start,
                "end_date": campaign_end,
                "influencer_id": influencer["influencer_id"],
                "influencer_name": influencer["name"],
                "followers": influencer["followers"],
                "promo_code": promo_code,
                "content_type": random.choice(["Post", "Story", "Video", "Reel"]),
                "fee": influencer["base_fee"] * random.uniform(0.9, 1.1),
                "target_audience": "Followers",
                "category": campaign_category,
                "performance_curve": self.generate_performance_curve(duration, is_influencer=True)
            }
            
            campaigns.append(campaign)
        
        return campaigns
    
    def generate_performance_curve(self, duration, is_influencer=False):
        """Generate a performance curve for campaign lifecycle
        
        Returns a dictionary with performance multipliers for different phases:
        - launch: Days 1-3, building momentum
        - growth: Days 4-10, increasing performance
        - peak: Middle period, optimal performance
        - decline: Final period, decreasing performance
        """
        # Influencer campaigns have sharper curves (spike and drop)
        if is_influencer:
            return {
                "launch": random.uniform(0.3, 0.5),  # Day 1 performance
                "growth_rate": random.uniform(1.5, 2.5),  # Daily growth factor
                "peak_day": min(duration - 1, random.randint(2, 5)),  # Peak performance day
                "peak_multiplier": random.uniform(1.0, 1.3),  # Peak performance multiplier
                "decay_rate": random.uniform(0.6, 0.8)  # Daily decay factor after peak
            }
        else:
            return {
                "launch": random.uniform(0.5, 0.7),  # Day 1 performance
                "growth_rate": random.uniform(1.1, 1.3),  # Daily growth factor
                "peak_day": min(duration - 1, random.randint(1, max(2, duration // 2))),  # Peak around middle
                "peak_multiplier": random.uniform(0.9, 1.1),  # Peak performance multiplier
                "decay_rate": random.uniform(0.95, 0.98)  # Daily decay factor after peak
            }
    
    def generate_senegalese_name(self):
        """Generate a random Senegalese name"""
        first_name = random.choice(senegalese_first_names)
        last_name = random.choice(senegalese_last_names)
        return f"{first_name} {last_name}"
    
    def generate_time_series(self):
        """Generate time series data for all campaigns"""
        if not self.active_campaigns["google"]:
            # Initialize if not already done
            self.initialize_campaigns()
        
        # Generate snapshots for each date in the range
        date_range = pd.date_range(start=self.start_date, end=self.end_date, freq=self.freq)
        
        # For each date, generate the day's performance data
        for current_date in tqdm(date_range, desc="Generating advertising time series"):
            self.current_date = current_date
            self.update_campaigns_for_date()
    
    def update_campaigns_for_date(self):
        """Update campaign data for the current date"""
        # Format date for filenames
        date_str = self.current_date.strftime('%Y%m%d')
        
        # 1. Check for campaign starts/ends and create new campaigns
        self.manage_campaign_lifecycle()
        
        # 2. Generate daily performance data for active campaigns
        google_data = self.generate_daily_google_data()
        social_data = self.generate_daily_social_data()
        influencer_data = self.generate_daily_influencer_data()
        
        # 3. Save daily snapshots
        if google_data:
            df = pd.DataFrame(google_data)
            df.to_csv(os.path.join(output_dir, f'google_ads_{date_str}.csv'), index=False)
            
        if social_data:
            df = pd.DataFrame(social_data)
            df.to_csv(os.path.join(output_dir, f'social_ads_{date_str}.csv'), index=False)
            
        if influencer_data:
            df = pd.DataFrame(influencer_data)
            df.to_csv(os.path.join(output_dir, f'influencer_data_{date_str}.csv'), index=False)
        
        # 4. Save combined platform data
        all_platform_data = []
        
        # Process Google Ads data
        for row in google_data:
            all_platform_data.append({
                "date": self.current_date.strftime('%Y-%m-%d'),
                "platform": "Google",
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
        for row in social_data:
            all_platform_data.append({
                "date": self.current_date.strftime('%Y-%m-%d'),
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
        
        # Process Influencer data
        for row in influencer_data:
            all_platform_data.append({
                "date": self.current_date.strftime('%Y-%m-%d'),
                "platform": f"Influencer_{row['platform']}",
                "campaign_id": row["campaign_id"],
                "campaign_name": row["campaign_name"],
                "ad_type": row["content_type"],
                "impressions": row["impressions"],
                "clicks": row["clicks"],
                "cost": row["fee"] / row["duration"],  # Daily cost
                "conversions": row["conversions"],
                "conversion_value": row["conversion_value"],
                "target_audience": "Followers",
                "category": row["category"]
            })
        
        if all_platform_data:
            df = pd.DataFrame(all_platform_data)
            df.to_csv(os.path.join(output_dir, f'all_platforms_{date_str}.csv'), index=False)
            
            # Also append to cumulative file
            all_platforms_path = os.path.join(output_dir, 'all_advertising_platforms.csv')
            if os.path.exists(all_platforms_path):
                df.to_csv(all_platforms_path, mode='a', header=False, index=False)
            else:
                df.to_csv(all_platforms_path, index=False)
        
        # Update campaign registry
        self.update_campaign_registry()
    
    def manage_campaign_lifecycle(self):
        """Manage campaign starts, ends, and create new campaigns"""
        # Check for campaign ends
        for platform in self.active_campaigns:
            active_list = []
            for campaign in self.active_campaigns[platform]:
                if campaign["end_date"] >= self.current_date:
                    active_list.append(campaign)
            
            # Replace with filtered list
            self.active_campaigns[platform] = active_list
        
        # Create new campaigns based on probabilistic events
        # Chance of new campaigns varies by day of week and season
        day_of_week = self.current_date.weekday()
        month = self.current_date.month
        
        # More campaign launches on Monday/Tuesday
        if day_of_week < 2:  # Monday or Tuesday
            new_campaign_chance = 0.3
        else:
            new_campaign_chance = 0.1
        
        # More campaigns during peak seasons
        if month in [4, 9, 12]:  # Ramadan, Tabaski, End of year
            new_campaign_chance *= 2
        
        # Create new campaigns with probabilities
        if random.random() < new_campaign_chance:
            # Determine which platform gets a new campaign
            platform_type = random.choices(
                ["google", "social", "influencer"],
                weights=[0.3, 0.5, 0.2]
            )[0]
            
            if platform_type == "google":
                new_campaign = self.generate_google_campaigns(1, self.current_date)[0]
                self.active_campaigns["google"].append(new_campaign)
            elif platform_type == "social":
                new_campaign = self.generate_social_campaigns(1, self.current_date)[0]
                self.active_campaigns["social"].append(new_campaign)
            else:
                new_campaign = self.generate_influencer_campaigns(1, self.current_date)[0]
                self.active_campaigns["influencer"].append(new_campaign)
    
    def generate_daily_google_data(self):
        """Generate daily performance data for Google campaigns"""
        daily_data = []
        
        for campaign in self.active_campaigns["google"]:
            # Check if campaign is active for current date
            if campaign["start_date"] <= self.current_date <= campaign["end_date"]:
                # Calculate day number in campaign lifecycle
                campaign_day = (self.current_date - campaign["start_date"]).days + 1
                campaign_duration = (campaign["end_date"] - campaign["start_date"]).days + 1
                
                # Get performance curve parameters
                curve = campaign["performance_curve"]
                
                # Calculate performance multiplier based on day in lifecycle
                if campaign_day <= curve["peak_day"]:
                    # Growth phase - exponential growth to peak
                    performance_multiplier = curve["launch"] * (curve["growth_rate"] ** (campaign_day - 1))
                else:
                    # Decay phase - exponential decay from peak
                    days_after_peak = campaign_day - curve["peak_day"]
                    peak_performance = curve["launch"] * (curve["growth_rate"] ** (curve["peak_day"] - 1))
                    performance_multiplier = peak_performance * (curve["decay_rate"] ** days_after_peak)
                
                # Adjust for weekday effect
                weekday = self.current_date.weekday()
                if weekday >= 5:  # Weekend
                    weekday_multiplier = 0.7  # Lower performance on weekends for B2B
                else:
                    weekday_multiplier = 1.0 + (1 - weekday/5) * 0.1  # Best on Monday, worst on Friday
                
                # Generate metrics for each ad type in Google Ads
                ad_types = ["Search", "Display", "Shopping"]
                
                for ad_type in ad_types:
                    # Base metrics calculation
                    avg_cpc = platforms["Google"]["avg_cpc"][ad_type] * random.uniform(0.9, 1.1)
                    daily_budget_adjusted = campaign["daily_budget"] * performance_multiplier * weekday_multiplier
                    
                    # Metrics calculation
                    impressions = int(daily_budget_adjusted / avg_cpc * random.uniform(15, 30))
                    ctr_min, ctr_max = platforms["Google"]["ctr_range"][ad_type]
                    ctr = random.uniform(ctr_min, ctr_max) * performance_multiplier
                    clicks = int(impressions * ctr)
                    cost = clicks * avg_cpc
                    
                    # Conversion metrics
                    conv_rate_min, conv_rate_max = platforms["Google"]["conversion_rate_range"][ad_type]
                    conversion_rate = random.uniform(conv_rate_min, conv_rate_max) * performance_multiplier
                    conversions = int(clicks * conversion_rate)
                    avg_order_value = random.uniform(8000, 20000)
                    conversion_value = conversions * avg_order_value
                    
                    # Daily record
                    record = {
                        "date": self.current_date.strftime("%Y-%m-%d"),
                        "platform": "Google",
                        "campaign_id": campaign["campaign_id"],
                        "campaign_name": campaign["campaign_name"],
                        "ad_type": ad_type,
                        "impressions": impressions,
                        "clicks": clicks,
                        "ctr": ctr,
                        "avg_cpc": avg_cpc,
                        "cost": cost,
                        "conversions": conversions,
                        "conversion_rate": conversion_rate,
                        "conversion_value": conversion_value,
                        "target_audience": campaign["target_audience"],
                        "category": campaign["category"],
                        "campaign_day": campaign_day,
                        "campaign_duration": campaign_duration
                    }
                    
                    # Add keywords if it's Search
                    if ad_type == "Search":
                        keywords = random.sample([
                            "cosmétiques naturels", "produits bio peau", "soins visage bio",
                            "karité pur", "huile baobab", "cosmétiques sénégal",
                            "soins cheveux naturels", "beauté bio afrique"
                        ], 3)
                        record["keywords"] = "|".join(keywords)
                    
                    daily_data.append(record)
        
        return daily_data
    
    def generate_daily_social_data(self):
        """Generate daily performance data for social media campaigns"""
        daily_data = []
        
        for campaign in self.active_campaigns["social"]:
            # Check if campaign is active for current date
            if campaign["start_date"] <= self.current_date <= campaign["end_date"]:
                # Calculate day number in campaign lifecycle
                campaign_day = (self.current_date - campaign["start_date"]).days + 1
                campaign_duration = (campaign["end_date"] - campaign["start_date"]).days + 1
                
                # Get performance curve parameters
                curve = campaign["performance_curve"]
                
                # Calculate performance multiplier based on day in lifecycle
                if campaign_day <= curve["peak_day"]:
                    # Growth phase - exponential growth to peak
                    performance_multiplier = curve["launch"] * (curve["growth_rate"] ** (campaign_day - 1))
                else:
                    # Decay phase - exponential decay from peak
                    days_after_peak = campaign_day - curve["peak_day"]
                    peak_performance = curve["launch"] * (curve["growth_rate"] ** (curve["peak_day"] - 1))
                    performance_multiplier = peak_performance * (curve["decay_rate"] ** days_after_peak)
                
                # Adjust for weekday effect (social better on evenings and weekends)
                weekday = self.current_date.weekday()
                if weekday >= 5:  # Weekend
                    weekday_multiplier = 1.2  # Better on weekends for social
                else:
                    weekday_multiplier = 0.9 + (weekday/5) * 0.2  # Worst on Monday, better later in week
                
                # Get ad types for this platform
                platform = campaign["platform"]
                
                # Handle TikTok which isn't in our platforms dictionary
                if platform == "TikTok":
                    ad_types = ["Video"]
                    avg_cpc_dict = {"Video": 350}
                    ctr_range_dict = {"Video": (0.03, 0.06)}
                    conv_rate_range_dict = {"Video": (0.02, 0.04)}
                else:
                    ad_types = platforms[platform]["ad_types"]
                    avg_cpc_dict = platforms[platform]["avg_cpc"]
                    ctr_range_dict = platforms[platform]["ctr_range"]
                    conv_rate_range_dict = platforms[platform]["conversion_rate_range"]
                
                for ad_type in ad_types:
                    # Social-specific metrics
                    avg_cpc = avg_cpc_dict[ad_type] * random.uniform(0.9, 1.1)
                    daily_budget_adjusted = campaign["daily_budget"] * performance_multiplier * weekday_multiplier
                    
                    # Core metrics
                    impressions = int(daily_budget_adjusted / avg_cpc * random.uniform(20, 40))
                    ctr_min, ctr_max = ctr_range_dict[ad_type]
                    ctr = random.uniform(ctr_min, ctr_max) * performance_multiplier
                    clicks = int(impressions * ctr)
                    cost = clicks * avg_cpc
                    
                    # Social engagement metrics
                    engagement_rate = ctr * random.uniform(1.5, 3)
                    engagements = int(impressions * engagement_rate)
                    likes = int(engagements * random.uniform(0.4, 0.6))
                    comments = int(engagements * random.uniform(0.05, 0.15))
                    shares = int(engagements * random.uniform(0.01, 0.1))
                    
                    # Conversion metrics
                    conv_rate_min, conv_rate_max = conv_rate_range_dict[ad_type]
                    conversion_rate = random.uniform(conv_rate_min, conv_rate_max) * performance_multiplier
                    conversions = int(clicks * conversion_rate)
                    avg_order_value = random.uniform(7500, 18000)
                    conversion_value = conversions * avg_order_value
                    
                    # Daily record
                    record = {
                        "date": self.current_date.strftime("%Y-%m-%d"),
                        "platform": platform,
                        "campaign_id": campaign["campaign_id"],
                        "campaign_name": campaign["campaign_name"],
                        "ad_type": ad_type,
                        "content_theme": campaign["content_theme"],
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
                        "target_audience": campaign["target_audience"],
                        "category": campaign["category"],
                        "campaign_day": campaign_day,
                        "campaign_duration": campaign_duration
                    }
                    
                    daily_data.append(record)
        
        return daily_data
    
    def generate_daily_influencer_data(self):
        """Generate daily performance data for influencer campaigns"""
        daily_data = []
        
        for campaign in self.active_campaigns["influencer"]:
            if campaign["start_date"] <= self.current_date <= campaign["end_date"]:
                campaign_day = (self.current_date - campaign["start_date"]).days + 1
                campaign_duration = (campaign["end_date"] - campaign["start_date"]).days + 1
                
                # Influencer curve is more spike-based - big impact on day of post, then quickly drops
                curve = campaign["performance_curve"]
                
                # Performance spike on specific days
                is_post_day = campaign_day == 1 or campaign_day % 7 == 0  # Post on day 1 and every 7 days
                
                if is_post_day:
                    # Spike day - full performance
                    performance_multiplier = 1.0
                else:
                    # Decay days - performance drops each day after post
                    days_since_post = campaign_day % 7 if campaign_day % 7 != 0 else 7
                    performance_multiplier = curve["decay_rate"] ** (days_since_post - 1)
                
                # Minimum floor for performance
                performance_multiplier = max(0.05, performance_multiplier)
                
                # Influencer metrics
                platform = campaign["platform"].replace("Influencer_", "")
                followers = campaign["followers"]
                
                # Reach calculation (not all followers see the post)
                if platform == "Instagram":
                    reach_rate = random.uniform(0.2, 0.35)
                elif platform == "TikTok":
                    reach_rate = random.uniform(0.1, 0.5)  # More variable on TikTok due to algorithm
                else:  # YouTube
                    reach_rate = random.uniform(0.1, 0.2)
                
                impressions = int(followers * reach_rate * performance_multiplier)
                
                # Engagement rate based on influencer tier and platform
                if "Micro" in campaign["influencer_id"]:
                    base_engagement = random.uniform(0.03, 0.06)
                elif "Mid" in campaign["influencer_id"]:
                    base_engagement = random.uniform(0.02, 0.045)
                else:  # Macro
                    base_engagement = random.uniform(0.015, 0.03)
                
                # Engagement metrics
                engagement_rate = base_engagement * performance_multiplier
                engagements = int(impressions * engagement_rate)
                
                # Click metrics (lower than regular ads)
                ctr = random.uniform(0.01, 0.04) * performance_multiplier
                clicks = int(impressions * ctr)
                
                # Conversion metrics (better than regular ads due to trust)
                conversion_rate = random.uniform(0.02, 0.06) * performance_multiplier
                conversions = int(clicks * conversion_rate)
                
                # Value metrics
                avg_order_value = random.uniform(9000, 18000)  # Higher AOV with influencers
                conversion_value = conversions * avg_order_value
                
                # Create record
                record = {
                    "date": self.current_date.strftime("%Y-%m-%d"),
                    "campaign_id": campaign["campaign_id"],
                    "campaign_name": campaign["campaign_name"],
                    "influencer_id": campaign["influencer_id"],
                    "influencer_name": campaign["influencer_name"],
                    "platform": platform,
                    "content_type": campaign["content_type"],
                    "impressions": impressions,
                    "engagements": engagements,
                    "engagement_rate": engagement_rate,
                    "clicks": clicks,
                    "ctr": ctr,
                    "conversions": conversions,
                    "conversion_rate": conversion_rate,
                    "conversion_value": conversion_value,
                    "fee": campaign["fee"],
                    "duration": campaign_duration,
                    "promo_code": campaign["promo_code"],
                    "category": campaign["category"],
                    "campaign_day": campaign_day,
                    "is_post_day": is_post_day
                }
                
                daily_data.append(record)
        
        return daily_data


# Main execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate advertising data with time progression')
    parser.add_argument('--start-date', default='2025-01-01', help='Start date for data generation (YYYY-MM-DD)')
    parser.add_argument('--end-date', default='2025-03-28', help='End date for data generation (YYYY-MM-DD)')
    parser.add_argument('--frequency', choices=['daily', 'weekly', 'monthly'], default='daily', 
                      help='Frequency of data snapshots')
    
    args = parser.parse_args()
    
    generator = AdvertisingDataGenerator(
        start_date=args.start_date,
        end_date=args.end_date,
        snapshot_frequency=args.frequency
    )
    
    generator.initialize_campaigns()
    generator.generate_time_series()
    
    print("Advertising data generation complete!")