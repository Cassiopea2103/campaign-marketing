import pandas as pd
import numpy as np
import uuid
import random
import datetime
import time
from faker import Faker
import json
import os
import argparse
import signal
import threading
import queue
from tqdm import tqdm
import glob

# Set up Faker with locale support
fake = Faker(['fr_FR'])  # French is widely spoken in Senegal
fake_sn = Faker(['en_US'])  # For non-locale specific items

# Create output directory if it doesn't exist
base_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(base_dir, '..', '..', 'data', 'raw', 'web')
os.makedirs(output_dir, exist_ok=True)
json_file_path = os.path.join(output_dir, 'web_logs.json')
csv_file_path = os.path.join(output_dir, 'web_logs.csv')


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

senegal_admin_structure = {
    "Dakar": {
        "region": "Dakar",
        "cities": ["Dakar", "Pikine", "Guédiawaye", "Rufisque", "Bargny", "Diamniadio"]
    },
    "Thiès": {
        "region": "Thiès",
        "cities": ["Thiès", "Mbour", "Tivaouane", "Joal-Fadiouth", "Kayar", "Pout"]
    },
    "Saint-Louis": {
        "region": "Saint-Louis",
        "cities": ["Saint-Louis", "Richard-Toll", "Dagana", "Podor"]
    },
    "Diourbel": {
        "region": "Diourbel",
        "cities": ["Diourbel", "Touba", "Mbacké", "Bambey"]
    },
    "Fatick": {
        "region": "Fatick",
        "cities": ["Fatick", "Kaolack", "Gossas", "Foundiougne"]
    },
    "Kaolack": {
        "region": "Kaolack",
        "cities": ["Kaolack", "Nioro du Rip", "Guinguinéo"]
    },
    "Kaffrine": {
        "region": "Kaffrine",
        "cities": ["Kaffrine", "Koungheul", "Malem Hodar"]
    },
    "Kédougou": {
        "region": "Kédougou",
        "cities": ["Kédougou", "Salémata", "Saraya"]
    },
    "Kolda": {
        "region": "Kolda",
        "cities": ["Kolda", "Vélingara", "Médina Yoro Foulah"]
    },
    "Louga": {
        "region": "Louga",
        "cities": ["Louga", "Linguère", "Kébémer"]
    },
    "Matam": {
        "region": "Matam",
        "cities": ["Matam", "Kanel", "Ranérou"]
    },
    "Sédhiou": {
        "region": "Sédhiou",
        "cities": ["Sédhiou", "Goudomp", "Bounkiling"]
    },
    "Tambacounda": {
        "region": "Tambacounda",
        "cities": ["Tambacounda", "Bakel", "Goudiry", "Koumpentoum"]
    },
    "Ziguinchor": {
        "region": "Ziguinchor",
        "cities": ["Ziguinchor", "Bignona", "Oussouye"]
    }
}
all_regions = list(senegal_admin_structure.keys())
all_cities = []
city_to_region_map = {}

for region, data in senegal_admin_structure.items():
    for city in data["cities"]:
        all_cities.append(city)
        city_to_region_map[city] = region


class WebEventGenerator:
    def __init__(self, start_date=None, end_date=None, real_time=False):
        """
        Initialize the web event generator with options for time-based generation
        
        Parameters:
        - start_date: Start date for historical data (YYYY-MM-DD)
        - end_date: End date for historical data (YYYY-MM-DD)  
        - real_time: If True, generate events at the current time, otherwise use the date range
        """
        self.real_time = real_time
        
        # For historical data generation
        self.start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d') if start_date else datetime.datetime(2025, 1, 1)
        self.end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.datetime(2025, 3, 28)
        self.current_date = self.start_date
        
        # Load reference data for consistency
        self.crm_ids, self.campaign_ids, self.crm_data = self.load_existing_ids()
        
        # Session management
        self.active_sessions = {}  # track ongoing sessions
        self.session_timeout = 30 * 60  # session timeout in seconds (30 minutes)
        
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
        self.coupons = [
            None, None, None,
            {"code": "TABASKI10", "discount": 10},
            {"code": "BIENVENUE", "discount": 10},
            {"code": "RAMADAN20", "discount": 20},
            {"code": "NOEL15", "discount": 15},
            {"code": "DECOUVERTE", "discount": 15}
        ]

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

    def load_existing_ids(self):
        """Load CRM and campaign IDs for data alignment"""
        crm_ids = []
        campaign_ids = []
        crm_data = {}
        
        # Load customer IDs from registry
        crm_registry = os.path.join(base_dir, '..', '..', 'data', 'raw', 'crm', 'customer_ids.txt')
        if os.path.exists(crm_registry):
            with open(crm_registry, 'r') as f:
                crm_ids = [line.strip() for line in f.readlines()]
        
        # Load campaign IDs from registry
        campaign_registry = os.path.join(base_dir, '..', '..', 'data', 'raw', 'advertising', 'campaign_ids.txt')
        if os.path.exists(campaign_registry):
            with open(campaign_registry, 'r') as f:
                campaign_ids = [line.strip() for line in f.readlines()]
        
        # Attempt to load detailed customer data for enhanced alignment
        try:
            # Find latest customers file
            customer_files = sorted(glob.glob(os.path.join(base_dir, '..', '..', 'data', 'raw', 'crm', 'customers_*.csv')))
            if customer_files:
                latest_file = customer_files[-1]
                customers_df = pd.read_csv(latest_file)
                
                # Create customer data dictionary
                for _, row in customers_df.iterrows():
                    crm_data[row['customer_id']] = {
                        'first_name': row['first_name'],
                        'last_name': row['last_name'],
                        'email': row['email'],
                        'phone': row.get('phone', ''),
                        'city': row.get('city', ''),
                        'region': row.get('region', ''),
                        'registration_date': row.get('registration_date', ''),
                        'favorite_category': row.get('favorite_category', '')
                    }
        except Exception as e:
            print(f"Warning: Could not load detailed customer data: {e}")
            
        print(f"Loaded {len(crm_ids)} customer IDs and {len(campaign_ids)} campaign IDs for alignment")
        return crm_ids, campaign_ids, crm_data
    
    def update_current_date(self, new_date=None):
        """Update the current date for historical generation"""
        if new_date:
            self.current_date = new_date
        else:
            # Move to next day
            self.current_date += datetime.timedelta(days=1)
            if self.current_date > self.end_date:
                self.current_date = self.start_date  # Wrap around
    
    def clean_expired_sessions(self, current_time=None):
        """Remove expired sessions based on timeout"""
        if current_time is None:
            current_time = datetime.datetime.now() if self.real_time else self.current_date
            
        expired_sessions = []
        
        for session_id, session in self.active_sessions.items():
            last_activity = session.get('last_activity')
            if last_activity:
                time_diff = (current_time - last_activity).total_seconds()
                if time_diff > self.session_timeout:
                    expired_sessions.append(session_id)
        
        # Remove expired sessions
        for session_id in expired_sessions:
            del self.active_sessions[session_id]
            
        return len(expired_sessions)
    
    def calculate_realistic_timestamp(self, base_time=None):
        """Calculate a realistic timestamp based on time of day patterns"""
        if base_time is None:
            base_time = datetime.datetime.now() if self.real_time else self.current_date
            
        # If historical, add random hour/minute/second
        if not self.real_time:
            # Hour weights based on typical e-commerce traffic patterns
            hour_weights = [1, 1, 1, 1, 1, 2, 2, 3, 5, 8, 10, 12, 12, 10, 11, 12, 14, 15, 20, 25, 22, 15, 8, 3]
            hour = random.choices(range(24), weights=hour_weights)[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            
            return base_time.replace(hour=hour, minute=minute, second=second)
        
        return base_time
    
    def generate_session_events(self, num_events=None):
        """Generate a complete user session with multiple events"""
        # Create or select session
        if random.random() < 0.8:  # 80% new sessions, 20% continue existing
            # Create new session
            session_id = str(uuid.uuid4())
            # Decide if user is authenticated
            is_authenticated = random.random() < 0.3  # 30% of sessions are authenticated
            user = self.generate_user(authenticated=is_authenticated)
            current_url = None  # Start at homepage
            events = []
        else:
            # Try to continue an existing session
            if not self.active_sessions:
                # No active sessions, create new
                session_id = str(uuid.uuid4())
                is_authenticated = random.random() < 0.3
                user = self.generate_user(authenticated=is_authenticated)
                current_url = None
                events = []
            else:
                # Select an active session
                session_id = random.choice(list(self.active_sessions.keys()))
                session = self.active_sessions[session_id]
                user = session.get('user')
                current_url = session.get('current_url')
                events = session.get('events', [])
        
        # Determine number of events for this generation cycle
        if num_events is None:
            # If continuing a session, fewer new events
            if current_url is not None:
                num_events = random.randint(1, 3)
            else:
                num_events = random.randint(1, 8)
        
        # Generate timestamp
        timestamp = self.calculate_realistic_timestamp()
        
        # Generate events
        new_events = []
        for i in range(num_events):
            # Events happen with small time increments
            if i > 0:
                timestamp += datetime.timedelta(seconds=random.randint(5, 180))
            
            # Generate event
            event = self.generate_event(
                session_id=session_id,
                user=user,
                current_url=current_url,
                timestamp=timestamp
            )
            
            # Update current URL for next event
            if "page" in event and "url" in event["page"]:
                current_url = event["page"]["url"].replace("https://www.biocosmetics.sn", "")
            elif "product" in event and "url" in event["product"]:
                current_url = event["product"]["url"].replace("https://www.biocosmetics.sn", "")
            
            new_events.append(event)
            events.append(event)
        
        # Update session data
        self.active_sessions[session_id] = {
            'user': user,
            'current_url': current_url,
            'last_activity': timestamp,
            'events': events
        }
        
        return new_events
    
    def generate_sn_ip(self):
        """Generate a realistic IP address (simulating Senegalese IPs)"""
        prefixes = ["41.82.", "41.83.", "154.124.", "196.1."]
        return f"{random.choice(prefixes)}{random.randint(0, 255)}.{random.randint(0, 255)}"
    
    def generate_senegal_email(self, first_name=None, last_name=None):
        """Generate an email with a Senegalese name and domain"""
        # Si les noms ne sont pas fournis, en générer de nouveaux
        if not first_name:
            first_name = random.choice(senegalese_first_names).lower()
        else:
            first_name = first_name.lower()
        
        if not last_name:
            last_name = random.choice(senegalese_last_names).lower()
        else:
            last_name = last_name.lower()
        
        domain = random.choice(self.senegal_email_domains)
    
        # Différents formats d'email
        formats = [
            f"{first_name}.{last_name}@{domain}",
            f"{first_name}{random.randint(1, 99)}@{domain}",
            f"{first_name[0]}{last_name}@{domain}",
            f"{last_name}.{first_name}@{domain}"
        ]
    
        return random.choice(formats)
    
    def generate_senegal_phone(self):
        """Generate a Senegalese phone number"""
        prefix = random.choice(['77', '78', '76', '70', '75'])
        suffix = ''.join([str(random.randint(0, 9)) for _ in range(7)])
        return f"+221{prefix}{suffix}"
    
    def generate_user(self, authenticated=None):
        """Generate a user (anonymous or authenticated)"""
        is_authenticated = authenticated if authenticated is not None else random.random() < 0.5 
        
        if is_authenticated:
            # Try to use a real customer ID from CRM for better data alignment
            if self.crm_ids and random.random() < 0.8:
                crm_id = random.choice(self.crm_ids)
                user_id = crm_id

                # Use the detailed CRM data if available
                if user_id in self.crm_data:
                    crm_user = self.crm_data[user_id]
                    user = {
                        "user_id": user_id,
                        "first_name": crm_user['first_name'],
                        "last_name": crm_user['last_name'],
                        "email": crm_user['email'],
                        "authenticated": True,
                        "registration_date": crm_user.get('registration_date', fake.date_time_this_year().isoformat()),
                        "user_segment": random.choice(["new", "regular", "vip", "diaspora"]),
                        "phone": crm_user.get('phone', self.generate_senegal_phone()),
                        "favorite_category": crm_user.get('favorite_category', random.choice(self.categories))
                    }
                    return user
                
            # Otherwise generate a synthetic user
            user_id = f"U{random.randint(10000, 99999)}"
            
            # Generate a Senegalese name
            first_name = random.choice(senegalese_first_names)
            last_name = random.choice(senegalese_last_names)
            
            # Generate email based on name
            email = self.generate_senegal_email(first_name, last_name)
            
            user = {
                "user_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "authenticated": True,
                "registration_date": fake.date_time_this_year().isoformat(),
                "user_segment": random.choice(["new", "regular", "vip", "diaspora"]),
                "phone": self.generate_senegal_phone(),
                "favorite_category": random.choice(self.categories)
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
            session_id = str(uuid.uuid4())
            
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
        
        
        ip_address = self.generate_sn_ip()
        is_mobile = "mobile" in device_type
        
        # Marketing attribution
        traffic_source = random.choice(self.traffic_sources)
        traffic_medium = random.choice(self.utm_mediums)
        
        # Choose a city, weighted by population
        city_weights = [self.cities.get(c, 0.01) for c in all_cities]
        city = random.choices(all_cities, weights=city_weights)[0]
        region = city_to_region_map.get(city, "Dakar")  # Default to Dakar if not found
        
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
                "region": region,
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
            
            # Check if the campaign already contains the ID or choose randomly
            current_campaign = event_base["marketing"]["campaign"]
            campaign_str = f"camp_{campaign_id}"
            
            # Verify if the current campaign already contains the ID or randomly choose
            if (current_campaign and campaign_str in current_campaign) or random.random() < 0.3:
                # Find a compatible source for this campaign
                for source, channels in self.ad_traffic_sources.items():
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
            
        elif event_type == "product_view":
            # For users with a favorite category, 70% chance to view products in that category
            if user.get('authenticated') and user.get('favorite_category') and random.random() < 0.7:
                favorite_category = user.get('favorite_category')
                category_products = [p for p in self.products if p["categorie"] == favorite_category]
                if category_products:
                    product = random.choice(category_products)
                else:
                    product = random.choice(self.products)
            else:
                # Otherwise select a random product
                product = random.choice(self.products)
            
            event_base["product"] = {
                "product_id": product["id"],
                "name": product["nom"],
                "price": product["prix"],
                "category": product["categorie"],
                "url": f"https://www.biocosmetics.sn/produits/{product['id']}"
            }
            
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
        
        elif event_type == "begin_checkout":
            # Items in cart
            num_items = random.randint(1, 4)
            cart_items = []
            cart_total = 0
            
            for _ in range(num_items):
                product = random.choice(self.products)
                quantity = random.randint(1, 2)
                item_price = product["prix"]
                cart_total += item_price * quantity
                
                cart_items.append({
                    "product_id": product["id"],
                    "name": product["nom"],
                    "price": product["prix"],
                    "category": product["categorie"],
                    "quantity": quantity
                })
            
            event_base["cart"] = {
                "items": cart_items,
                "total": cart_total,
                "coupon": random.choice(self.coupons)
            }
            
        elif event_type == "purchase":
            # Items purchased
            num_items = random.randint(1, 4)
            purchased_items = []
            purchase_total = 0
            
            for _ in range(num_items):
                product = random.choice(self.products)
                quantity = random.randint(1, 2)
                item_price = product["prix"]
                purchase_total += item_price * quantity
                
                purchased_items.append({
                    "product_id": product["id"],
                    "name": product["nom"],
                    "price": product["prix"],
                    "category": product["categorie"],
                    "quantity": quantity
                })
            
            # Apply discount
            coupon_obj = random.choice(self.coupons)
            discount = 0
            if coupon_obj:
                discount_percent = coupon_obj["discount"]
                coupon = coupon_obj["code"]
                discount = int(purchase_total * discount_percent / 100)
            # Shipping cost
            shipping_cost = 0 if purchase_total > 25000 else random.choice([2000, 3000, 4000])
            
            # Payment method
            payment_method = random.choices(
                list(self.payment_methods.keys()),
                weights=list(self.payment_methods.values())
            )[0]
            
            event_base["purchase"] = {
                "order_id": f"ORD-{uuid.uuid4().hex[:8]}",
                "items": purchased_items,
                "subtotal": purchase_total,
                "discount": discount,
                "coupon": coupon,
                "shipping": shipping_cost,
                "total": purchase_total - discount + shipping_cost,
                "payment_method": payment_method,
                "currency": "XOF"  # CFA Franc
            }
            
        elif event_type == "search":
            # Search terms related to cosmetics in French
            search_terms = [
                "crème hydratante",
                "huile de karité",
                "savon noir",
                "soin visage",
                "masque argile",
                "anti-taches",
                "huile de baobab",
                "shampoing naturel",
                "maquillage peau noire",
                "parfum bio"
            ]
            
            event_base["search"] = {
                "query": random.choice(search_terms),
                "results_count": random.randint(0, 12)
            }
            
        elif event_type == "filter_products":
            # Filter options
            filter_options = {
                "category": random.choice(self.categories),
                "price_range": random.choice(["0-5000", "5000-10000", "10000-20000", "20000+"]),
                "sort_by": random.choice(["popularity", "price_asc", "price_desc", "newest"]),
                "rating": random.choice(["4+", "3+", "all"])
            }
            
            event_base["filter"] = filter_options
        
        return event_base
        
    def run_continuous_generation(self, output_format='file', events_per_minute=50, duration_seconds=None):
        """
        Run continuous event generation
        
        Parameters:
        - output_format: 'file' or 'kafka'
        - events_per_minute: Target events per minute
        - duration_seconds: How long to run, or None for indefinite
        """
        # Set up signal handling for graceful shutdown
        self.running = True
        
        def signal_handler(sig, frame):
            print("Shutting down web log generator...")
            self.running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        
        # Calculate timing
        seconds_per_event = 60.0 / events_per_minute
        
        # Track statistics
        start_time = time.time()
        event_count = 0
        session_count = 0
        
        print(f"Starting continuous web log generation at {events_per_minute} events per minute...")
        print("Press Ctrl+C to stop")
        
        # Event queue for asynchronous writing
        event_queue = queue.Queue(maxsize=1000)
        
        # Writer thread function
        def event_writer():
            while self.running or not event_queue.empty():
                try:
                    batch = []
                    # Get up to 100 events at once if available
                    for _ in range(100):
                        if not event_queue.empty():
                            batch.append(event_queue.get(block=False))
                            event_queue.task_done()
                        else:
                            break
                    
                    if batch:
                        self.write_events(batch, output_format)
                    else:
                        time.sleep(0.1)  # Short sleep if no events
                except Exception as e:
                    print(f"Error in writer thread: {e}")
                    time.sleep(1)  # Delay on error
        
        # Start writer thread
        writer_thread = threading.Thread(target=event_writer, daemon=True)
        writer_thread.start()
        
        try:
            # Main generation loop
            while self.running:
                # Check if duration has expired
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    print(f"Reached specified duration of {duration_seconds} seconds")
                    self.running = False
                    break
                
                # Clean expired sessions periodically
                if event_count % 100 == 0:
                    expired = self.clean_expired_sessions()
                    if expired > 0:
                        print(f"Cleaned {expired} expired sessions")
                
                # Generate a batch of events for a session
                events = self.generate_session_events()
                event_count += len(events)
                session_count += 1
                
                # Queue events for writing
                for event in events:
                    event_queue.put(event)
                
                # Calculate sleep time to maintain target rate
                # Adjust for the number of events generated
                target_sleep = seconds_per_event * len(events)
                
                # Add some randomness to make it more realistic
                jitter = random.uniform(-0.1, 0.1) * target_sleep
                sleep_time = max(0.01, target_sleep + jitter)
                
                # Sleep
                time.sleep(sleep_time)
                
                # Periodic status update
                if event_count % 500 == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed
                    print(f"Generated {event_count} events in {elapsed:.1f} seconds ({rate:.1f} events/sec)")
        
        finally:
            # Ensure writer processes remaining events
            print("Waiting for writer to finish...")
            event_queue.join()
            print(f"Generation complete: {event_count} events across {session_count} sessions")
    
    def write_events(self, events, output_format):
        """Write events to the specified output format"""
        if not events:
            return
        
        # Get timestamp for filenames
        timestamp = datetime.datetime.now().strftime('%Y%m%d')
        
        if output_format == 'file':
            # Write to timestamped JSON file
            json_file_path = os.path.join(output_dir, f'web_logs_{timestamp}.json')
            with open(json_file_path, 'a', encoding='utf-8') as f:
                for event in events:
                    f.write(json.dumps(event, ensure_ascii=False) + '\n')
                    
            # Also update a cumulative CSV for easier analysis
            # This is inefficient for high-volume streaming but useful for demo purposes
            try:
                # Flatten the event for CSV (top-level fields only)
                flat_events = []
                for event in events:
                    flat_event = {
                        'event_id': event.get('event_id'),
                        'event_type': event.get('event_type'),
                        'timestamp': event.get('timestamp'),
                        'session_id': event.get('session_id'),
                        'user_id': event.get('user', {}).get('user_id'),
                        'authenticated': event.get('user', {}).get('authenticated', False),
                        'device_type': event.get('device', {}).get('type'),
                        'browser': event.get('device', {}).get('browser'),
                        'is_mobile': event.get('device', {}).get('is_mobile', False),
                        'country': event.get('location', {}).get('country'),
                        'city': event.get('location', {}).get('city'),
                        'region': event.get('location', {}).get('region'),
                        'source': event.get('marketing', {}).get('source'),
                        'campaign': event.get('marketing', {}).get('campaign'),
                        'medium': event.get('marketing', {}).get('medium'),
                        'campaign_id': event.get('marketing', {}).get('campaign_id')
                    }
                    
                    # Add page-specific fields if available
                    if 'page' in event:
                        flat_event['page_url'] = event['page'].get('url')
                        flat_event['referrer'] = event['page'].get('referrer')
                        flat_event['visit_duration'] = event['page'].get('visit_duration')
                    
                    # Add product-specific fields if available
                    if 'product' in event:
                        flat_event['product_id'] = event['product'].get('product_id')
                        flat_event['product_name'] = event['product'].get('name')
                        flat_event['product_price'] = event['product'].get('price')
                        flat_event['product_category'] = event['product'].get('category')
                        flat_event['quantity'] = event['product'].get('quantity')
                    
                    flat_events.append(flat_event)
                
                # Create DataFrame and append to CSV
                df = pd.DataFrame(flat_events)
                csv_file_path = os.path.join(output_dir, f'web_logs_{timestamp}.csv')
                
                # Write with or without header
                if not os.path.exists(csv_file_path):
                    df.to_csv(csv_file_path, index=False)
                else:
                    df.to_csv(csv_file_path, mode='a', header=False, index=False)
                    
            except Exception as e:
                print(f"Warning: CSV conversion error: {e}")
                
        elif output_format == 'kafka':
            # Kafka integration - can be implemented based on your Kafka setup
            # For example using kafka-python:
            # from kafka import KafkaProducer
            # producer = KafkaProducer(bootstrap_servers='localhost:9092')
            # for event in events:
            #     producer.send('web_logs', json.dumps(event).encode('utf-8'))
            pass
                
    def generate_historical_data(self, start_date, end_date, daily_event_count=5000):
        """Generate historical data for a date range"""
        print(f"Generating historical web logs from {start_date} to {end_date}")
        
        # Set up date range
        self.start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        self.current_date = self.start_date
        self.real_time = False
        
        # Process each day
        current_date = self.start_date
        total_events = 0
        
        while current_date <= self.end_date:
            # Update generator's current date
            self.current_date = current_date
            date_str = current_date.strftime('%Y%m%d')
            
            print(f"Generating data for {current_date.strftime('%Y-%m-%d')}...")
            
            # Calculate event count for this day with some variation
            # Weekends have less traffic
            weekday = current_date.weekday()
            weekend_factor = 0.7 if weekday >= 5 else 1.0
            
            # Special days have more traffic
            month = current_date.month
            day = current_date.day
            
            # Boost factors for special periods
            special_factor = 1.0
            
            # Ramadan effect (approximate)
            if month == 4:
                special_factor = 1.3
            
            # Tabaski effect (approximate)
            elif month == 9 and day > 15:
                special_factor = 1.4
            
            # End of year shopping
            elif month == 12:
                if day < 15:
                    special_factor = 1.2
                else:
                    special_factor = 1.5  # Higher closer to holidays
            
            # Sales periods
            elif month == 2 or month == 7:
                special_factor = 1.2
            
            # Calculate final event count with randomness
            day_event_count = int(daily_event_count * weekend_factor * special_factor * random.uniform(0.9, 1.1))
            
            # Generate events for the day
            day_events = []
            
            # Generate in sessions for more realism
            sessions_to_generate = day_event_count // 5  # Average 5 events per session
            
            for _ in tqdm(range(sessions_to_generate), desc=f"Generating {day_event_count} events", unit="session"):
                # Random events per session (1-10)
                session_event_count = min(10, max(1, int(np.random.poisson(5))))
                
                # Generate all events for this session
                session_events = self.generate_session_events(session_event_count)
                day_events.extend(session_events)
                
                # Short delay to prevent resource exhaustion
                if _ % 100 == 0:
                    time.sleep(0.01)
            
            # Write all events for the day
            self.write_events(day_events, 'file')
            
            total_events += len(day_events)
            print(f"Generated {len(day_events)} events for {current_date.strftime('%Y-%m-%d')}")
            
            # Move to next day
            current_date += datetime.timedelta(days=1)
            
            # Clean up sessions between days
            self.active_sessions = {}
        
        print(f"Historical data generation complete: {total_events} events")


# Main execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate web log data with time progression')
    parser.add_argument('--mode', choices=['batch', 'stream'], default='batch',
                      help='Generation mode: batch (historical) or stream (continuous)')
    parser.add_argument('--start-date', default='2025-01-01', help='Start date for historical data (YYYY-MM-DD)')
    parser.add_argument('--end-date', default='2025-03-28', help='End date for historical data (YYYY-MM-DD)')
    parser.add_argument('--events-per-day', type=int, default=5000, help='Average events per day for historical mode')
    parser.add_argument('--events-per-minute', type=int, default=50, help='Events per minute for streaming mode')
    parser.add_argument('--duration', type=int, default=None, help='Duration in seconds for streaming mode')
    parser.add_argument('--output', choices=['file', 'kafka'], default='file',
                      help='Output format (file or kafka)')
    
    args = parser.parse_args()
    
    # Create generator
    generator = WebEventGenerator(start_date=args.start_date, end_date=args.end_date, real_time=(args.mode == 'stream'))
    
    if args.mode == 'batch':
        # Generate historical data
        generator.generate_historical_data(
            start_date=args.start_date,
            end_date=args.end_date,
            daily_event_count=args.events_per_day
        )
    else:
        # Run continuous generation
        generator.run_continuous_generation(
            output_format=args.output,
            events_per_minute=args.events_per_minute,
            duration_seconds=args.duration
        )