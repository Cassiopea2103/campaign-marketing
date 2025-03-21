import pandas as pd
import numpy as np
import uuid
import random
import datetime
import time
from faker import Faker
import json
import os

# Set up Faker with locale support
fake = Faker(['fr_FR'])  # French is widely spoken in Senegal
fake_sn = Faker(['en_US'])  # For non-locale specific items

# Create output directory if it doesn't exist
base_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(base_dir, '..' , '..',  'data', 'raw')
os.makedirs(output_dir, exist_ok=True)

class WebEventGenerator:
    def __init__(self, crm_ids=None, campaign_ids=None):
        """Initialize the web event generator with optional CRM and campaign IDs for data alignment"""
        # Allow injection of IDs from other sources for alignment
        self.crm_ids = crm_ids or []  # Customer IDs from CRM
        self.campaign_ids = campaign_ids or []  # Campaign IDs from advertising data
        
        # Define product categories and products - including popular cosmetics in Senegal
        self.categories = ["soin_visage", "soin_corps", "soin_cheveux", "maquillage", "parfum", "homme", "bio", "traditionnel"]
        self.products = [
            {"id": "P1001", "nom": "Crème hydratante au karité", "prix": 12500, "categorie": "soin_visage"},
            {"id": "P1002", "nom": "Sérum à l'huile de baobab", "prix": 18000, "categorie": "soin_visage"},
            {"id": "P1003", "nom": "Huile de coco pressée à froid", "prix": 9500, "categorie": "soin_corps"},
            {"id": "P1004", "nom": "Savon noir traditionnel", "prix": 4000, "categorie": "soin_corps"},
            {"id": "P1005", "nom": "Masque purifiant à l'argile du Sahel", "prix": 11000, "categorie": "soin_visage"},
            {"id": "P1006", "nom": "Beurre de karité pur", "prix": 7500, "categorie": "soin_corps"},
            {"id": "P1007", "nom": "Huile d'argan bio", "prix": 15000, "categorie": "soin_cheveux"},
            {"id": "P1008", "nom": "Crème solaire naturelle SPF30", "prix": 13500, "categorie": "soin_corps"},
            {"id": "P1009", "nom": "Baume à lèvres au miel", "prix": 3500, "categorie": "soin_visage"},
            {"id": "P1010", "nom": "Eau florale de rose du Maghreb", "prix": 9000, "categorie": "soin_visage"},
            {"id": "P1011", "nom": "Henné naturel du Sénégal", "prix": 5000, "categorie": "soin_cheveux"},
            {"id": "P1012", "nom": "Lotion après-rasage au moringa", "prix": 8000, "categorie": "homme"},
            {"id": "P1013", "nom": "Shampooing aux extraits de kola", "prix": 6500, "categorie": "soin_cheveux"},
            {"id": "P1014", "nom": "Gommage au sel de mer de Dakar", "prix": 10000, "categorie": "soin_corps"},
            {"id": "P1015", "nom": "Dentifrice naturel au siwak", "prix": 4500, "categorie": "traditionnel"},
            {"id": "P1016", "nom": "Fond de teint peau foncée", "prix": 15000, "categorie": "maquillage"},
            {"id": "P1017", "nom": "Rouge à lèvres naturel", "prix": 7500, "categorie": "maquillage"},
            {"id": "P1018", "nom": "Mascara végétal", "prix": 9000, "categorie": "maquillage"},
            {"id": "P1019", "nom": "Parfum à l'hibiscus", "prix": 18000, "categorie": "parfum"},
            {"id": "P1020", "nom": "Eau de baobab", "prix": 14000, "categorie": "parfum"}
        ]
        
        # Product pages mapping
        self.product_pages = {product["id"]: f"/produits/{product['id']}" for product in self.products}
        self.category_pages = {cat: f"/categories/{cat}" for cat in self.categories}
        
        # Basic pages
        self.pages = [
            "/", "/produits", "/categories", "/panier", "/mon-compte",
            "/checkout", "/confirmation", "/support", "/a-propos", "/blog"
        ]
        
        # Senegalese cities with population-based weights
        self.cities = {
            "Dakar": 0.35, "Thiès": 0.15, "Touba": 0.12, "Rufisque": 0.08, 
            "Saint-Louis": 0.07, "Mbour": 0.05, "Kaolack": 0.05, "Ziguinchor": 0.04, 
            "Diourbel": 0.03, "Louga": 0.03, "Tambacounda": 0.03
        }
        
        # Device information - mobile higher proportion for Senegal
        self.devices = {
            "mobile_android": 0.6, "mobile_ios": 0.15, 
            "desktop_windows": 0.1, "desktop_mac": 0.05, 
            "tablet_android": 0.07, "tablet_ios": 0.03
        }
        
        self.browsers = {
            "Chrome Mobile": 0.4, "Opera Mini": 0.15, "Chrome": 0.2, 
            "Firefox": 0.1, "Safari": 0.07, "Edge": 0.03, 
            "Mobile Safari": 0.03, "UC Browser": 0.02
        }
        
        # Telecom operators in Senegal
        self.telecom_operators = ["Orange", "Free", "Expresso"]
        
        # Traffic sources
        self.traffic_sources = [
            "direct", "google", "facebook", "whatsapp", "instagram", "youtube",
            "jumia", "expat-dakar.com", "seneweb.com", "tiktok", "telegram", 
            "orange.sn", "refer-friend", "email", "sms"
        ]
        
        # UTM parameters for tracking
        self.utm_sources = ["facebook", "instagram", "whatsapp", "google", "youtube", "seneweb", "dakaractu", "jumia"]
        self.utm_mediums = ["cpc", "email", "social", "banner", "sms", "referral", "organic", "direct"]
        self.utm_campaigns = ["ramadan_2025", "journee_femme", "ete_2025", "tabaski_promo", "rentree_2025", "fin_annee"]
        
        # Event types
        self.event_types = [
            "page_view", "product_view", "add_to_cart", "remove_from_cart", 
            "begin_checkout", "purchase", "search", "filter_products", "login", "signup"
        ]
        
        # Payment methods common in Senegal
        self.payment_methods = {
            "orange_money": 0.35, "free_money": 0.20, "wave": 0.25, 
            "card": 0.10, "cash_on_delivery": 0.08, "paypal": 0.02
        }
        
        # Coupons
        self.coupons = [None, None, None, "TABASKI10", "BIENVENUE", "RAMADAN20", "NOEL15", "DECOUVERTE"]

        self.senegal_email_domains = ["orange.sn", "gmail.com", "yahoo.fr", "hotmail.fr", "outlook.com", "free.sn"]

        # Average durations for pages
        self.page_avg_durations = {
            "/": (20, 60),  # (min, max) in seconds
            "/produits": (30, 180),
            "/categories": (20, 90),
            "/panier": (40, 120),
            "/mon-compte": (30, 150),
            "/checkout": (60, 300),
            "/confirmation": (10, 40),
            "/support": (40, 240),
            "/a-propos": (15, 60),
            "/blog": (60, 360)
        }
        
        # Ad traffic sources used in marketing campaigns
        self.ad_traffic_sources = {
            "facebook": ["fb_newsfeed", "fb_stories", "instagram", "fb_marketplace"],
            "google": ["search", "display", "youtube", "discovery"],
            "tiktok": ["feed", "topview", "hashtag_challenge"],
            "email": ["newsletter", "promo", "abandoned_cart", "welcome"]
        }
        
        # Shipping zones
        self.shipping_zones = {
            "Dakar Centre": {"cost": 1000, "free_threshold": 15000},
            "Dakar Périphérie": {"cost": 2000, "free_threshold": 20000},
            "Grand Dakar": {"cost": 3000, "free_threshold": 25000},
            "Autres régions": {"cost": 5000, "free_threshold": 30000}
        }

    def generate_sn_ip(self):
        """Generate a realistic IP address (simulating Senegalese IPs)"""
        prefixes = ["41.82.", "41.83.", "154.124.", "196.1."]
        return f"{random.choice(prefixes)}{random.randint(0, 255)}.{random.randint(0, 255)}"
    
    def generate_session(self):
        """Generate a new user session ID"""
        return str(uuid.uuid4())
    
    def generate_senegal_email(self):
        """Generate an email with a local name and domain"""
        name = fake.user_name().lower().replace(' ', '.')
        domain = random.choice(self.senegal_email_domains)
        return f"{name}@{domain}"
    
    def generate_senegal_phone(self):
        """Generate a Senegalese phone number"""
        prefix = random.choice(['77', '78', '76', '70', '75'])
        suffix = ''.join([str(random.randint(0, 9)) for _ in range(7)])
        return f"+221{prefix}{suffix}"
        
    def generate_user(self, authenticated=None):
        """Generate a user (anonymous or authenticated)"""
        is_authenticated = authenticated if authenticated is not None else random.random() < 0.3
        
        if is_authenticated:
            # If we have CRM IDs available, use one for alignment
            if self.crm_ids and random.random() < 0.8:  # 80% chance to use an existing CRM ID
                crm_id = random.choice(self.crm_ids)
                user_id = f"U{crm_id}"  # Prefix U to indicate it's a web user ID
            else:
                user_id = f"U{random.randint(10000, 99999)}"
                
            user = {
                "user_id": user_id,
                "email": self.generate_senegal_email(),
                "authenticated": True,
                "registration_date": fake.date_time_this_year().isoformat(),
                "user_segment": random.choice(["new", "regular", "vip", "diaspora"]),
                "phone": self.generate_senegal_phone()
            }
        else:
            user = {
                "user_id": None,
                "authenticated": False
            }
        
        return user
    
    def generate_event(self, session_id=None, user=None, current_url=None, timestamp=None):
        """Generate a web event"""
        # Create a new session if not provided
        if not session_id:
            session_id = self.generate_session()
            
        # Create a user if not provided
        if not user:
            user = self.generate_user()
        
        # Use provided timestamp or generate one
        if not timestamp:
            # Generate timestamp with more events during evening hours for Senegal
            hour_weights = [1, 1, 1, 1, 1, 2, 2, 3, 5, 8, 10, 12, 12, 10, 11, 12, 14, 15, 20, 25, 22, 15, 8, 3]
            hour = random.choices(range(24), weights=hour_weights)[0]
            timestamp = fake.date_time_this_month().replace(hour=hour)
        
        # Type of event
        if current_url is None:
            # First event in session is typically a page view
            event_type = "page_view"
            current_url = "/"
        else:
            # Determine event type based on current URL
            if "/produits/" in current_url:
                # Product page - mostly product interactions
                event_type = random.choices(
                    ["page_view", "product_view", "add_to_cart", "remove_from_cart"],
                    weights=[0.2, 0.3, 0.4, 0.1]
                )[0]
            elif "/categories/" in current_url:
                # Category page - mostly navigation and product views
                event_type = random.choices(
                    ["page_view", "product_view", "filter_products", "search"],
                    weights=[0.3, 0.3, 0.2, 0.2]
                )[0]
            elif current_url == "/panier":
                # Cart page - checkout or continue shopping
                event_type = random.choices(
                    ["page_view", "remove_from_cart", "begin_checkout"],
                    weights=[0.2, 0.3, 0.5]
                )[0]
            elif current_url == "/checkout":
                # Checkout page - purchase or abandon
                event_type = random.choices(
                    ["page_view", "purchase", "begin_checkout"],
                    weights=[0.3, 0.4, 0.3]
                )[0]
            else:
                # Other pages - general browsing
                event_type = random.choices(
                    self.event_types,
                    weights=[0.5, 0.2, 0.1, 0.05, 0.05, 0.02, 0.03, 0.02, 0.02, 0.01]
                )[0]

        # Base common for all events
        device_type = random.choices(
            list(self.devices.keys()),
            weights=list(self.devices.values())
        )[0]
        
        browser = random.choices(
            list(self.browsers.keys()),
            weights=list(self.browsers.values())
        )[0]
        
        city = random.choices(
            list(self.cities.keys()),
            weights=list(self.cities.values())
        )[0]
        
        ip_address = self.generate_sn_ip()
        is_mobile = "mobile" in device_type
        
        # Marketing attribution
        traffic_source = random.choice(self.traffic_sources)
        traffic_medium = random.choice(self.utm_mediums)
        
        # Base event data
        event_base = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": timestamp.isoformat(),
            "session_id": session_id,
            "user": user,
            "device": {
                "type": device_type,
                "browser": browser,
                "os": random.choice(["Android", "Android", "iOS", "Windows", "MacOS", "Linux"]),  # More Android
                "resolution": random.choice(["360x640", "375x667", "414x896", "1366x768", "1280x720", "1920x1080"]),
                "operator": random.choice(self.telecom_operators) if random.random() < 0.7 else None,
                "is_mobile": is_mobile
            },
            "location": {
                "country": "Sénégal",
                "city": city,
                "region": random.choice(["Dakar", "Thiès", "Saint-Louis", "Diourbel", "Fatick", "Kaolack", "Kaffrine", 
                                       "Kédougou", "Kolda", "Louga", "Matam", "Sédhiou", "Tambacounda", "Ziguinchor"]),
                "ip_address": ip_address,
                "language": "fr" if random.random() < 0.9 else "en"  # 90% French, 10% English
            },
            "marketing": {
                "source": traffic_source,
                "medium": traffic_medium,
                "campaign": random.choice([None, "tabaski_2025", "ramadan_promo", "nouveaux_produits", "reactivation"])
            }
        }
        
        # Add campaign reference if applicable
        if self.campaign_ids and random.random() < 0.4:  # 40% chance to be linked to a campaign
            campaign_id = random.choice(self.campaign_ids)
            selected_source = None
            
            # Check if the campaign already contains the ID or choose randomly
            current_campaign = event_base["marketing"]["campaign"]
            campaign_str = f"camp_{campaign_id}"
            
            # Verify if the current campaign already contains the ID or randomly choose
            if (current_campaign and campaign_str in current_campaign) or random.random() < 0.3:
                # Find a compatible source for this campaign
                for source, channels in self.ad_traffic_sources.items():
                    selected_source = source
                    event_base["marketing"]["source"] = source
                    event_base["marketing"]["campaign"] = campaign_str
                    event_base["marketing"]["channel"] = random.choice(channels)
                    event_base["marketing"]["campaign_id"] = campaign_id
                    break
        
        # Page/URL data
        if event_type == "page_view":
            # Determine appropriate URL based on context
            if not current_url or current_url == "/":
                page_url = random.choice(self.pages)
            else:
                page_url = current_url
                
            # Add page visit duration
            min_duration, max_duration = self.page_avg_durations.get(
                page_url.split('?')[0],  # Remove query parameters for duration lookup
                (10, 120)  # Default if page not found
            )
            duration = random.randint(min_duration, max_duration)
            
            # Add referrer for first page in session
            if not current_url:  # First page in session
                if event_base["marketing"]["source"] == "google":
                    referrer = "https://www.google.sn/search"
                elif event_base["marketing"]["source"] in ["facebook", "instagram", "whatsapp"]:
                    referrer = f"https://www.{event_base['marketing']['source']}.com/"
                elif event_base["marketing"]["source"] == "seneweb":
                    referrer = "https://www.seneweb.com/"
                elif event_base["marketing"]["source"] == "email":
                    referrer = None  # Direct from email
                else:
                    referrer = "direct"
            else:
                referrer = "https://www.biocosmetics.sn" + current_url
            
            event_base["page"] = {
                "url": f"https://www.biocosmetics.sn{page_url}",
                "referrer": referrer,
                "title": f"BioCosmetics Sénégal - {page_url.replace('/', '').capitalize() or 'Accueil'}",
                "visit_duration": duration  # Page visit duration in seconds
            }
            
            # Determine the next URL for this session's flow
            next_url = page_url
        
        elif event_type == "product_view":
            # Select a product
            product = random.choice(self.products)
            
            event_base["product"] = {
                "product_id": product["id"],
                "name": product["nom"],
                "price": product["prix"],
                "category": product["categorie"],
                "url": f"https://www.biocosmetics.sn/produits/{product['id']}"
            }
            
            # Next URL is the product page
            next_url = f"/produits/{product['id']}"
            
        elif event_type in ["add_to_cart", "remove_from_cart"]:
            # Select a product
            product = random.choice(self.products)
            quantity = random.randint(1, 3)
                
            event_base["product"] = {
                "product_id": product["id"],
                "name": product["nom"],
                "price": product["prix"],
                "category": product["categorie"],
                "quantity": quantity
            }
        return event_base


def generate_web_logs(num_sessions):
    """Generate web logs for the specified number of sessions"""
    logs_data = []
        
    # Create generator instance
    generator = WebEventGenerator()
        
    for _ in range(num_sessions):
        # Create a new session
        session_id = generator.generate_session()
            
        # Decide if user is authenticated
        is_authenticated = random.random() < 0.3  # 30% of sessions are authenticated
        user = generator.generate_user(authenticated=is_authenticated)
            
        # Generate between 1 and 20 events per session
        num_events = random.randint(1, 20)
            
        # Session timestamp (starting point)
        session_start = fake.date_time_this_month()
            
        # Track current URL as we navigate
        current_url = None
            
        for i in range(num_events):
            # Events happen with small time increments (1-5 minutes)
            current_time = session_start + datetime.timedelta(minutes=i*random.randint(1, 5))
                
            # Generate event
            event = generator.generate_event(
                session_id=session_id, 
                user=user, 
                current_url=current_url, 
                timestamp=current_time
            )
                
            # Check if event is not None before processing
            if event is not None:
                # Update current URL for next event
                if "page" in event and "url" in event["page"]:
                    current_url = event["page"]["url"].replace("https://www.biocosmetics.sn", "")
                elif "product" in event and "url" in event["product"]:
                    current_url = event["product"]["url"].replace("https://www.biocosmetics.sn", "")
                    
                logs_data.append(event)
        
    return logs_data

# Generate logs
num_sessions = 5000  # Adjust as needed
logs_data = generate_web_logs(num_sessions)

# Save as JSON (for streaming simulation)
with open(os.path.join(output_dir, 'web_logs.json'), 'w') as f:
    for log in logs_data:
        f.write(json.dumps(log) + '\n')

# Also save as CSV for easier analysis
df = pd.DataFrame(logs_data)
df.to_csv('data/raw/web_logs.csv', index=False)

print(f"Generated {len(logs_data)} web log events from {num_sessions} sessions")
print(f"Files saved to 'data/web_logs.json' and 'data/web_logs.csv'")

# Preview some data
print("\nPreview of generated data:")
print(df.head())