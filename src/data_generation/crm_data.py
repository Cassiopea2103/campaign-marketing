import pandas as pd
import numpy as np
import uuid
import random
import datetime
from faker import Faker
import os
import glob
import argparse
from tqdm import tqdm

# Set up Faker with locale support
fake = Faker(['fr_FR'])  
fake_en = Faker(['en_US']) 
Faker.seed(42)  # For reproducibility

# Create output directory if it doesn't exist
base_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(base_dir, '..', '..', 'data', 'raw', 'crm')
os.makedirs(output_dir, exist_ok=True)


# senegalese names : 
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

def generate_senegalese_name():
    return random.choice(senegalese_first_names), random.choice(senegalese_last_names)

# Define Senegalese cities with population-based weights
cities = {
    "Dakar": 0.35, "Thiès": 0.15, "Touba": 0.12, "Rufisque": 0.08, 
    "Saint-Louis": 0.07, "Mbour": 0.05, "Kaolack": 0.05, "Ziguinchor": 0.04, 
    "Diourbel": 0.03, "Louga": 0.03, "Tambacounda": 0.03
}

def generate_senegal_address(city):
    """Génère une adresse au format sénégalais cohérente avec la ville"""
        
    # Types de voies au Sénégal
    street_types = ["Avenue", "Rue", "Boulevard", "Allée", "Place", "Route de"]
        
    # Quartiers par ville (ajouter selon besoin)
    neighborhoods = {
        "Dakar": ["Plateau", "Médina", "Fann", "Mermoz", "Sacré-Cœur", "Ouakam", "Almadies", "Yoff", "Ngor", "Point E", "Liberté"],
        "Thiès": ["Randoulène", "Cité Ballabey", "Mbour 1", "Mbour 2", "Thialy"],
        "Touba": ["Darou Marnane", "Darou Miname", "Guédé", "Ndamatou"],
        "Rufisque": ["Diokoul", "Keury Souf", "Keury Kaw", "Colobane"],
        "Saint-Louis": ["Guet Ndar", "Sor", "Île Nord", "Île Sud", "Ndiolofène"],
        "Mbour": ["Escale", "Mbour Sérère", "Diamaguène", "Téfess"],
        "Kaolack": ["Médina Baye", "Leona", "Kasaville", "Dialègne"],
    }
        
    # Noms sénégalais pour les rues (personnalités, lieux, etc.)
    street_names = [
        "Léopold Sédar Senghor", "Blaise Diagne", "Lat Dior", "El Hadj Malick Sy", 
        "Cheikh Anta Diop", "Samory Touré", "Lamine Guèye", "Macky Sall", 
        "Abdou Diouf", "Ousmane Sonko", "Abdoulaye Wade", "Serigne Touba",
        "de l'Indépendance", "de la République", "du Sahel", "de la Liberté",
        "de la Paix", "de l'Unité Africaine", "du Baobab", "des Niayes"
    ]
        
    # Choisir éléments pour l'adresse
    street_type = random.choice(street_types)
    street_name = random.choice(street_names)
        
    # Choisir un quartier correspondant à la ville, ou générer un nom générique si la ville n'est pas listée
    if city in neighborhoods:
        neighborhood = random.choice(neighborhoods[city])
    else:
        neighborhood = f"Quartier {random.choice(['Centre', 'Nord', 'Sud', 'Est', 'Ouest', 'Nouveau'])}"
        
    # Numéro de rue (moins élevé que dans les adresses françaises)
    number = random.randint(1, 120)
        
    # Format d'adresse sénégalais (avec variations)
    address_formats = [
        f"{number}, {street_type} {street_name}, {neighborhood}",
        f"{street_type} {street_name}, N°{number}, {neighborhood}",
        f"Villa N°{number}, {street_type} {street_name}, {neighborhood}",
        f"{neighborhood}, {number} {street_type} {street_name}",
        f"{street_type} {street_name}, {neighborhood}, Lot N°{number}"
    ]
        
    return random.choice(address_formats)


# Senegalese phone number formats
def generate_senegal_phone():
    # Préfixes mobiles sénégalais
    prefixes = ["77", "78", "76", "70", "75"]
    return f"+221 {random.choice(prefixes)} {random.randint(100, 999)} {random.randint(10, 99)} {random.randint(10, 99)}"

# Skin types and concerns relevant to local demographic
skin_types = ["Normal", "Sec", "Mixte", "Gras", "Sensible", "Mature"]
skin_concerns = ["Hyperpigmentation", "Acné", "Taches", "Teint terne", "Brillance", "Rides", "Sécheresse", 
                "Sensibilité", "Protection solaire", "Cicatrices", "Grain de peau irrégulier"]

# Popular ingredients in Senegalese and African cosmetics
ingredients = ["Beurre de karité", "Huile de baobab", "Bissap", "Moringa", "Huile de coco", 
              "Beurre de cacao", "Aloe vera", "Argile", "Huile d'argan", "Huile de ricin",
              "Huile de neem", "Beurre de mangue", "Ximenia", "Henné", "Huile de palme"]

# Common allergies and sensitivities
allergies = ["Parfums synthétiques", "Alcool", "Sulfates", "Parabènes", "Silicones", "Huiles minérales", 
            "Lanoline", "Phénoxyéthanol", "Colorants artificiels", "Allergène alimentaire"]

# Payment methods common in Senegal
payment_methods = {
    "Orange Money": 0.30, 
    "Wave": 0.25, 
    "Free Money": 0.15, 
    "Carte bancaire": 0.15, 
    "Espèces à la livraison": 0.10, 
    "PayPal": 0.05
}

# Generate customer data
def generate_customers(num_customers=1000):
    customers = []
    
    for _ in range(num_customers):
        customer_id = str(uuid.uuid4())

        gender = random.choices(["F", "M", "Autre"], weights=[0.8, 0.15, 0.05])[0]
        male_names = [name for name in senegalese_first_names if name not in ["Fatou", "Aminata", "Aïssatou", "Rokhaya", "Mariama", "Awa", "Khady", "Dieynaba", "Sokhna", "Ndeye", "Astou", "Fatoumata", "Rama", "Mame", "Adja", "Sophie", "Coumba", "Nabou", "Soda", "Bineta", "Yacine", "Bintou", "Fama", "Ramatoulaye", "Safiétou", "Dior", "Yaye", "Tening", "Mbayang", "Penda", "Maty", "Kiné", "Seynabou", "Fari", "Adjara", "Salimata", "Marième", "Anta", "Saly", "Oumy", "Marème", "Tida", "Diarra", "Ndèye", "Diouma", "Magatte", "Ndella", "Kadija", "Maïmouna", "Tata", "Ndioro", "Yandé", "Diama", "Codou", "Bérénice", "Aissatou", "Amy", "Ngoné", "Mbathio"]]
        female_names = ["Fatou", "Aminata", "Aïssatou", "Rokhaya", "Mariama", "Awa", "Khady", "Dieynaba", "Sokhna", "Ndeye", "Astou", "Fatoumata", "Rama", "Mame", "Adja", "Sophie", "Coumba", "Nabou", "Soda", "Bineta", "Yacine", "Bintou", "Fama", "Ramatoulaye", "Safiétou", "Dior", "Yaye", "Tening", "Mbayang", "Penda", "Maty", "Kiné", "Seynabou", "Fari", "Adjara", "Salimata", "Marième", "Anta", "Saly", "Oumy", "Marème", "Tida", "Diarra", "Ndèye", "Diouma", "Magatte", "Ndella", "Kadija", "Maïmouna", "Tata", "Ndioro", "Yandé", "Diama", "Codou", "Bérénice", "Aissatou", "Amy", "Ngoné", "Mbathio"]
        
        if gender == "F":
            first_name = random.choice(female_names)
        elif gender == "M":
            first_name = random.choice(male_names)
        
        # Basic information
        first_name, last_name = generate_senegalese_name()
        email_domain = random.choice(['gmail.com', 'yahoo.fr', 'hotmail.com', 'outlook.com'])
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}@{email_domain}"
        phone = generate_senegal_phone()
        
        # Location (weighted by population)
        city = random.choices(list(cities.keys()), weights=list(cities.values()))[0]

        # Get the corresponding region for the city
        region = city_to_region_map.get(city, "Dakar")  # Default to Dakar if not found

        
        # Registration and purchase dates
        registration_date = fake.date_time_between(start_date='-3y', end_date='now')
        
        # 90% of customers have made at least one purchase
        has_purchased = random.random() < 0.9
        
        if has_purchased:
            first_purchase_date = fake.date_time_between(start_date=registration_date, end_date='now')
            last_purchase_date = fake.date_time_between(start_date=first_purchase_date, end_date='now')
            total_orders = random.randint(1, 20)  # Some customers are very loyal
            
            # Higher lifetime value for customers with more orders
            base_order_value = random.randint(5000, 25000)  # In CFA francs (approx. $8-$40)
            lifetime_value = base_order_value * total_orders * (1 + random.random())
        else:
            first_purchase_date = None
            last_purchase_date = None
            total_orders = 0
            lifetime_value = 0
        
        # Customer preferences
        favorite_category = random.choice(["soin_visage", "soin_corps", "soin_cheveux", "maquillage", "parfum"])
        skin_type = random.choice(skin_types)
        
        # Multiple skin concerns
        num_concerns = random.randint(1, 3)
        selected_concerns = random.sample(skin_concerns, num_concerns)
        
        # Preferred ingredients
        num_preferred = random.randint(1, 4)
        preferred_ingredients = random.sample(ingredients, num_preferred)
        
        # Allergies (only 30% have allergies)
        if random.random() < 0.3:
            num_allergies = random.randint(1, 2)
            customer_allergies = random.sample(allergies, num_allergies)
        else:
            customer_allergies = []
            
        # Age groups common for cosmetics customers
        age = random.choices([
            f"{random.randint(18, 24)}", 
            f"{random.randint(25, 34)}", 
            f"{random.randint(35, 44)}", 
            f"{random.randint(45, 54)}", 
            f"{random.randint(55, 65)}"
        ], weights=[0.2, 0.35, 0.25, 0.15, 0.05])[0]
        
        address = generate_senegal_address(city) if has_purchased else None
        
        # Email subscription and engagement
        is_subscribed = random.random() < 0.75  # 75% are subscribed to emails
        email_engagement = random.choices(["Élevé", "Moyen", "Faible"], weights=[0.3, 0.4, 0.3])[0] if is_subscribed else "Non abonné"
        
        # Create customer entry
        customer = {
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
            "city": city,
            "region": region,
            "address": address,
            "registration_date": registration_date.strftime("%Y-%m-%d %H:%M:%S"),
            "first_purchase_date": first_purchase_date.strftime("%Y-%m-%d %H:%M:%S") if first_purchase_date else None,
            "last_purchase_date": last_purchase_date.strftime("%Y-%m-%d %H:%M:%S") if last_purchase_date else None,
            "total_orders": total_orders,
            "lifetime_value": int(lifetime_value),
            "favorite_category": favorite_category,
            "skin_type": skin_type,
            "skin_concerns": "|".join(selected_concerns),
            "preferred_ingredients": "|".join(preferred_ingredients),
            "allergies": "|".join(customer_allergies),
            "age": age,
            "gender": gender,
            "is_subscribed": is_subscribed,
            "email_engagement": email_engagement,
            "acquisition_source": random.choice(["Instagram", "Facebook", "Référence", "Google", "WhatsApp", "Événement"])
        }
        
        customers.append(customer)
    
    return customers

# Generate order data
def generate_orders(customers_df, num_orders=5000):
    orders = []
    date_columns = ['registration_date', 'first_purchase_date', 'last_purchase_date']
    for col in date_columns:
        if col in customers_df.columns:
            customers_df[col] = pd.to_datetime(customers_df[col], errors='coerce')

    # Get customers who have made purchases
    customers_with_orders = customers_df[customers_df['total_orders'] > 0].copy()
    
    # Product categories and items
    categories = ["soin_visage", "soin_corps", "soin_cheveux", "maquillage", "parfum"]
    products_by_category = {
        "soin_visage": [
            {"name": "Sérum au Karité", "price": 12000},
            {"name": "Crème au Baobab", "price": 8500},
            {"name": "Masque à l'Argile", "price": 5000},
            {"name": "Nettoyant au Moringa", "price": 6500},
            {"name": "Toner au Bissap", "price": 7000}
        ],
        "soin_corps": [
            {"name": "Beurre de Karité", "price": 9000},
            {"name": "Huile de Baobab", "price": 11000},
            {"name": "Gommage au Café", "price": 7500},
            {"name": "Lait à la Coco", "price": 8000},
            {"name": "Savon Noir", "price": 3500}
        ],
        "soin_cheveux": [
            {"name": "Huile de Ricin", "price": 10000},
            {"name": "Masque à l'Avocat", "price": 8500},
            {"name": "Shampoing Naturel", "price": 7000},
            {"name": "Beurre de Karité Cheveux", "price": 9500},
            {"name": "Spray à l'Hibiscus", "price": 6500}
        ],
        "maquillage": [
            {"name": "Fond de Teint Peau Foncée", "price": 15000},
            {"name": "Rouge à Lèvres Naturel", "price": 7500},
            {"name": "Mascara Végétal", "price": 9000},
            {"name": "Poudre Latérite", "price": 8500},
            {"name": "Eye Liner Noir", "price": 6000}
        ],
        "parfum": [
            {"name": "Parfum à l'Hibiscus", "price": 18000},
            {"name": "Eau de Baobab", "price": 14000},
            {"name": "Cologne Agrumes", "price": 12000},
            {"name": "Huile de Jasmin", "price": 11000},
            {"name": "Encens Africain", "price": 8500}
        ]
    }
    
    # Promotional seasons and codes
    promo_seasons = {
        "normal": {"weight": 0.6, "discount_range": (0, 0.1)},
        "ramadan": {"weight": 0.1, "discount_range": (0.1, 0.25)},
        "tabaski": {"weight": 0.1, "discount_range": (0.1, 0.25)},
        "fin_annee": {"weight": 0.1, "discount_range": (0.15, 0.3)},
        "soldes": {"weight": 0.1, "discount_range": (0.2, 0.5)}
    }
    
    promo_codes = {
        "normal": ["MERCI10", "BIENVENUE", "FIDELITE"],
        "ramadan": ["RAMADAN20", "EID15", "FTOUR"],
        "tabaski": ["TABASKI20", "AID25", "CELEBRATION"],
        "fin_annee": ["NOEL25", "NOUVEL_AN", "CADEAU20"],
        "soldes": ["SOLDES30", "PROMO50", "BONPLAN"]
    }
    
    # UTM parameters
    utm_sources = ["facebook", "instagram", "whatsapp", "google", "email", "direct"]
    utm_mediums = ["cpc", "email", "social", "organic", "referral"]
    utm_campaigns = [None, "ramadan_2025", "tabaski_promo", "beaute_naturelle", "lancement_produit"]
    
    # Order statuses with weights
    order_statuses = {
        "Livré": 0.8,
        "En cours": 0.1,
        "Annulé": 0.05,
        "Remboursé": 0.05
    }
    
    # Generate orders
    for i in range(num_orders):
        # Select a customer (weighted to give some customers multiple orders)
        customer = customers_with_orders.sample(weights=customers_with_orders['total_orders']).iloc[0]
        
        # Order date
        try:
            if pd.notna(customer['first_purchase_date']) and pd.notna(customer['last_purchase_date']):
                # Convertir en objets datetime si ce ne sont pas déjà des datetime
                if isinstance(customer['first_purchase_date'], (str, tuple)):
                    start_date = pd.to_datetime(customer['first_purchase_date'])
                else:
                    start_date = customer['first_purchase_date']
                
                if isinstance(customer['last_purchase_date'], (str, tuple)):
                    end_date = pd.to_datetime(customer['last_purchase_date'])
                else:
                    end_date = customer['last_purchase_date']
                
                # Générer date aléatoire entre les deux dates
                days_range = (end_date - start_date).days
                if days_range > 0:
                    random_days = random.randint(0, days_range)
                    order_date = start_date + datetime.timedelta(days=random_days)
                else:
                    order_date = start_date  # Même jour
            else:
                # Fallback si dates manquantes
                order_date = fake.date_time_between(start_date='-2y', end_date='now')
        except Exception as e:
            print(f"Erreur lors du traitement des dates: {e}")
            print(f"Valeurs: first_purchase_date={customer['first_purchase_date']}, last_purchase_date={customer['last_purchase_date']}")
            # Utiliser une date par défaut
            order_date = fake.date_time_between(start_date='-2y', end_date='now')
        
        # Determine season based on date
        month = order_date.month
        if 9 <= month <= 10:  # Tabaski approximate time (varies yearly)
            season_weights = {"normal": 0.3, "tabaski": 0.7, "ramadan": 0, "fin_annee": 0, "soldes": 0}
        elif month == 4:  # Ramadan approximate time (varies yearly)
            season_weights = {"normal": 0.3, "tabaski": 0, "ramadan": 0.7, "fin_annee": 0, "soldes": 0}
        elif month == 12 or month == 1:  # End of year
            season_weights = {"normal": 0.2, "tabaski": 0, "ramadan": 0, "fin_annee": 0.8, "soldes": 0}
        elif month == 7 or month == 2:  # Sales periods
            season_weights = {"normal": 0.3, "tabaski": 0, "ramadan": 0, "fin_annee": 0, "soldes": 0.7}
        else:
            season_weights = {"normal": 0.8, "tabaski": 0, "ramadan": 0, "fin_annee": 0, "soldes": 0.2}
            
        # Select season based on weights
        season = random.choices(
            list(season_weights.keys()),
            weights=list(season_weights.values())
        )[0]
        
        # Apply discount based on season
        has_discount = random.random() < 0.4  # 40% of orders have a discount
        
        if has_discount:
            discount_min, discount_max = promo_seasons[season]["discount_range"]
            discount_percentage = random.uniform(discount_min, discount_max)
            discount_code = random.choice(promo_codes[season]) if random.random() < 0.7 else None
        else:
            discount_percentage = 0
            discount_code = None
        # Generate 1-5 items per order
        num_items = random.choices([1, 2, 3, 4, 5], weights=[0.4, 0.3, 0.2, 0.07, 0.03])[0]
        
        # Prefer customer's favorite category if available
        favorite_category = customer['favorite_category']
        
        # Create items
        order_items = []
        order_total = 0
        
        for _ in range(num_items):
            if random.random() < 0.7 and favorite_category in categories:
                # 70% chance to choose from favorite category
                category = favorite_category
            else:
                category = random.choice(categories)
                
            # Select product
            product = random.choice(products_by_category[category])
            product_name = product["name"]
            product_price = product["price"]
            quantity = random.choices([1, 2, 3], weights=[0.8, 0.15, 0.05])[0]
            
            item_total = product_price * quantity
            order_total += item_total
            
            item = {
                "product_name": product_name,
                "category": category,
                "price": product_price,
                "quantity": quantity,
                "item_total": item_total
            }
            
            order_items.append(item)
        
        # Apply discount
        discount_amount = int(order_total * discount_percentage)
        final_total = order_total - discount_amount
        
        # Shipping cost (free over 25000 CFA)
        shipping_cost = 0 if final_total > 25000 else random.choice([2000, 3000, 4000])
        
        # Payment method
        payment_method = random.choices(
            list(payment_methods.keys()),
            weights=list(payment_methods.values())
        )[0]
        
        # Order status (more recent orders more likely to be in progress)
        days_since_order = (datetime.datetime.now() - order_date).days
        
        if days_since_order < 3:
            status_weights = {"Livré": 0.2, "En cours": 0.8, "Annulé": 0, "Remboursé": 0}
        elif days_since_order < 7:
            status_weights = {"Livré": 0.7, "En cours": 0.2, "Annulé": 0.05, "Remboursé": 0.05}
        else:
            status_weights = {"Livré": 0.9, "En cours": 0, "Annulé": 0.05, "Remboursé": 0.05}
            
        order_status = random.choices(
            list(status_weights.keys()),
            weights=list(status_weights.values())
        )[0]
        
        # UTM parameters (for tracking)
        has_utm = random.random() < 0.7  # 70% of orders have tracking
        
        if has_utm:
            utm_source = random.choice(utm_sources)
            utm_medium = random.choice(utm_mediums)
            utm_campaign = random.choice(utm_campaigns)
        else:
            utm_source = utm_medium = utm_campaign = None
        
        # Create order entry
        order = {
            "order_id": f"ORD-{i+10000}",
            "customer_id": customer['customer_id'],
            "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "order_total": order_total,
            "discount_amount": discount_amount,
            "discount_code": discount_code,
            "final_total": final_total,
            "shipping_cost": shipping_cost,
            "payment_method": payment_method,
            "order_status": order_status,
            "utm_source": utm_source,
            "utm_medium": utm_medium,
            "utm_campaign": utm_campaign,
            "items": order_items,
            "season": season,
            "city": customer['city'],
            "phone": customer['phone']
        }
        
        orders.append(order)
    
    return orders

def generate_orders_for_date(customers_df, num_orders, order_date):
    """Generate orders for a specific date"""
    
    # Copy of generate_orders function but with fixed date logic
    orders = []
    
    # Get customers who have made purchases or are likely to purchase
    potential_customers = customers_df.copy()
    potential_customers['purchase_weight'] = 1.0
    
    # Weight by customer attributes
    for idx, customer in potential_customers.iterrows():
        # Customers with no orders
        if customer['total_orders'] == 0:
            # 5% chance for first purchase
            potential_customers.at[idx, 'purchase_weight'] = 0.05
        else:
            try:
                # Existing customers - higher weight for recent purchasers
                if pd.notna(customer['last_purchase_date']):
                    last_purchase = pd.to_datetime(customer['last_purchase_date'])
                    days_since_purchase = (order_date - last_purchase).days
                    
                    # Exponential decay based on days since last purchase
                    recency_factor = np.exp(-days_since_purchase / 30)  # 30 day half-life
                    
                    # Higher weight for valuable customers
                    value_factor = min(2.0, max(0.5, customer['lifetime_value'] / 50000))
                    
                    potential_customers.at[idx, 'purchase_weight'] = recency_factor * value_factor
                else:
                    potential_customers.at[idx, 'purchase_weight'] = 0.1
            except:
                potential_customers.at[idx, 'purchase_weight'] = 0.1
    
    # Product categories and items
    categories = ["soin_visage", "soin_corps", "soin_cheveux", "maquillage", "parfum"]
    products_by_category = {
        "soin_visage": [
            {"name": "Sérum au Karité", "price": 12000},
            {"name": "Crème au Baobab", "price": 8500},
            {"name": "Masque à l'Argile", "price": 5000},
            {"name": "Nettoyant au Moringa", "price": 6500},
            {"name": "Toner au Bissap", "price": 7000}
        ],
        "soin_corps": [
            {"name": "Beurre de Karité", "price": 9000},
            {"name": "Huile de Baobab", "price": 11000},
            {"name": "Gommage au Café", "price": 7500},
            {"name": "Lait à la Coco", "price": 8000},
            {"name": "Savon Noir", "price": 3500}
        ],
        "soin_cheveux": [
            {"name": "Huile de Ricin", "price": 10000},
            {"name": "Masque à l'Avocat", "price": 8500},
            {"name": "Shampoing Naturel", "price": 7000},
            {"name": "Beurre de Karité Cheveux", "price": 9500},
            {"name": "Spray à l'Hibiscus", "price": 6500}
        ],
        "maquillage": [
            {"name": "Fond de Teint Peau Foncée", "price": 15000},
            {"name": "Rouge à Lèvres Naturel", "price": 7500},
            {"name": "Mascara Végétal", "price": 9000},
            {"name": "Poudre Latérite", "price": 8500},
            {"name": "Eye Liner Noir", "price": 6000}
        ],
        "parfum": [
            {"name": "Parfum à l'Hibiscus", "price": 18000},
            {"name": "Eau de Baobab", "price": 14000},
            {"name": "Cologne Agrumes", "price": 12000},
            {"name": "Huile de Jasmin", "price": 11000},
            {"name": "Encens Africain", "price": 8500}
        ]
    }
    
    # Promotional seasons and codes
    promo_seasons = {
        "normal": {"weight": 0.6, "discount_range": (0, 0.1)},
        "ramadan": {"weight": 0.1, "discount_range": (0.1, 0.25)},
        "tabaski": {"weight": 0.1, "discount_range": (0.1, 0.25)},
        "fin_annee": {"weight": 0.1, "discount_range": (0.15, 0.3)},
        "soldes": {"weight": 0.1, "discount_range": (0.2, 0.5)}
    }
    
    promo_codes = {
        "normal": ["MERCI10", "BIENVENUE", "FIDELITE"],
        "ramadan": ["RAMADAN20", "EID15", "FTOUR"],
        "tabaski": ["TABASKI20", "AID25", "CELEBRATION"],
        "fin_annee": ["NOEL25", "NOUVEL_AN", "CADEAU20"],
        "soldes": ["SOLDES30", "PROMO50", "BONPLAN"]
    }
    
    # UTM parameters
    utm_sources = ["facebook", "instagram", "whatsapp", "google", "email", "direct"]
    utm_mediums = ["cpc", "email", "social", "organic", "referral"]
    utm_campaigns = [None, "ramadan_2025", "tabaski_promo", "beaute_naturelle", "lancement_produit"]
    
    # Generate orders for the specified date
    for i in range(num_orders):
        # Select a customer based on purchase probability
        if len(potential_customers) > 0:
            customer = potential_customers.sample(weights=potential_customers['purchase_weight']).iloc[0]
            
            # Add a random hour to make the timestamp more realistic
            hour = random.randint(8, 22)  # Orders between 8am and 10pm
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            order_datetime = order_date.replace(hour=hour, minute=minute, second=second)
            
            # Determine season based on date
            month = order_date.month
            if 9 <= month <= 10:  # Tabaski approximate time
                season = "tabaski"
            elif month == 4:  # Ramadan approximate time
                season = "ramadan"
            elif month == 12 or month == 1:  # End of year
                season = "fin_annee"
            elif month == 7 or month == 2:  # Sales periods
                season = "soldes"
            else:
                season = "normal"
                
            # Apply discount based on season
            has_discount = random.random() < 0.4  # 40% of orders have a discount
            
            if has_discount:
                discount_min, discount_max = promo_seasons[season]["discount_range"]
                discount_percentage = random.uniform(discount_min, discount_max)
                discount_code = random.choice(promo_codes[season]) if random.random() < 0.7 else None
            else:
                discount_percentage = 0
                discount_code = None
            
            # Generate 1-5 items per order
            num_items = random.choices([1, 2, 3, 4, 5], weights=[0.4, 0.3, 0.2, 0.07, 0.03])[0]
            
            # Prefer customer's favorite category if available
            favorite_category = customer['favorite_category']
            
            # Create items
            order_items = []
            order_total = 0
            
            for _ in range(num_items):
                if random.random() < 0.7 and favorite_category in categories:
                    # 70% chance to choose from favorite category
                    category = favorite_category
                else:
                    category = random.choice(categories)
                    
                # Select product
                product = random.choice(products_by_category[category])
                product_name = product["name"]
                product_price = product["price"]
                quantity = random.choices([1, 2, 3], weights=[0.8, 0.15, 0.05])[0]
                
                item_total = product_price * quantity
                order_total += item_total
                
                item = {
                    "product_name": product_name,
                    "category": category,
                    "price": product_price,
                    "quantity": quantity,
                    "item_total": item_total
                }
                
                order_items.append(item)
            
            # Apply discount
            discount_amount = int(order_total * discount_percentage)
            final_total = order_total - discount_amount
            
            # Shipping cost (free over 25000 CFA)
            shipping_cost = 0 if final_total > 25000 else random.choice([2000, 3000, 4000])
            
            # Payment method
            payment_method = random.choices(
                list(payment_methods.keys()),
                weights=list(payment_methods.values())
            )[0]
            
            # Order status (based on current date proximity)
            days_since_order = (datetime.datetime.now() - order_datetime).days
            
            if days_since_order < 3:
                status_weights = {"Livré": 0.2, "En cours": 0.8, "Annulé": 0, "Remboursé": 0}
            elif days_since_order < 7:
                status_weights = {"Livré": 0.7, "En cours": 0.2, "Annulé": 0.05, "Remboursé": 0.05}
            else:
                status_weights = {"Livré": 0.9, "En cours": 0, "Annulé": 0.05, "Remboursé": 0.05}
                
            order_status = random.choices(
                list(status_weights.keys()),
                weights=list(status_weights.values())
            )[0]
            
            # UTM parameters (for tracking)
            has_utm = random.random() < 0.7  # 70% of orders have tracking
            
            if has_utm:
                utm_source = random.choice(utm_sources)
                utm_medium = random.choice(utm_mediums)
                utm_campaign = random.choice(utm_campaigns)
            else:
                utm_source = utm_medium = utm_campaign = None
            
            # Create order entry
            order = {
                "order_id": f"ORD-{uuid.uuid4().hex[:8]}",
                "customer_id": customer['customer_id'],
                "order_date": order_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                "order_total": order_total,
                "discount_amount": discount_amount,
                "discount_code": discount_code,
                "final_total": final_total,
                "shipping_cost": shipping_cost,
                "payment_method": payment_method,
                "order_status": order_status,
                "utm_source": utm_source,
                "utm_medium": utm_medium,
                "utm_campaign": utm_campaign,
                "items": order_items,
                "season": season,
                "city": customer['city'],
                "phone": customer['phone']
            }
            
            orders.append(order)
    
    return orders

class CRMDataGenerator:
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
            
        # Initialize customer and order databases
        self.customers_df = None
        self.orders_df = None
        
        # Track current simulation date
        self.current_date = self.start_date
        
        # Maintain a registry of customer IDs for reference by other scripts
        self.customer_id_registry = os.path.join(output_dir, 'customer_ids.txt')
        
        # Customer acquisition rates by month (higher during holiday seasons)
        self.acquisition_rates = {
            1: 2,    # January (post-holiday)
            2: 2,    # February
            3: 3,    # March
            4: 4,    # April (Ramadan)
            5: 3,    # May
            6: 2,    # June
            7: 3,    # July
            8: 2,    # August
            9: 5,    # September (Tabaski)
            10: 4,   # October
            11: 3,   # November
            12: 6    # December (holiday season)
        }
        
    def initialize_data(self, num_customers=500):
        """Create initial customer base"""
        print(f"Initializing CRM data with {num_customers} customers...")
        
        # Generate initial customers
        customers_data = generate_customers(num_customers)
        self.customers_df = pd.DataFrame(customers_data)
        
        # Save initial customer database
        initial_date = self.start_date.strftime('%Y%m%d')
        self.customers_df.to_csv(os.path.join(output_dir, f'customers_{initial_date}.csv'), index=False)
        
        # Initialize orders (empty at start, will be populated)
        self.orders_df = pd.DataFrame()
        
        # Save customer IDs to registry file for other scripts
        with open(self.customer_id_registry, 'w') as f:
            for customer_id in self.customers_df['customer_id']:
                f.write(f"{customer_id}\n")
                
        print(f"Initial CRM data saved with prefix {initial_date}")
        
    def generate_time_series(self):
        """Generate time series data for the specified date range"""
        if self.customers_df is None:
            # Initialize if not already done
            self.initialize_data()
            
        # Generate snapshots for each date in the range
        date_range = pd.date_range(start=self.start_date, end=self.end_date, freq=self.freq)
        
        for current_date in tqdm(date_range, desc="Generating CRM time series"):
            self.current_date = current_date
            self.update_data_for_date()
            
    def update_data_for_date(self):
        """Update customer and order data for the current date"""
        # Format date for filenames
        date_str = self.current_date.strftime('%Y%m%d')
        
        # 1. Add new customers
        self.add_new_customers()
        
        # 2. Generate daily orders
        self.generate_daily_orders()
        
        # 3. Save updated data
        self.customers_df.to_csv(os.path.join(output_dir, f'customers_{date_str}.csv'), index=False)
        
        # Save only today's orders
        today_orders = self.orders_df[self.orders_df['order_date'].str.contains(self.current_date.strftime('%Y-%m-%d'))]
        if not today_orders.empty:
            today_orders.to_csv(os.path.join(output_dir, f'orders_{date_str}.csv'), index=False)
            
            # Also append to cumulative orders file
            all_orders_path = os.path.join(output_dir, 'all_orders.csv')
            if os.path.exists(all_orders_path):
                today_orders.to_csv(all_orders_path, mode='a', header=False, index=False)
            else:
                today_orders.to_csv(all_orders_path, index=False)
        
        # Update customer ID registry
        with open(self.customer_id_registry, 'w') as f:
            for customer_id in self.customers_df['customer_id']:
                f.write(f"{customer_id}\n")
                
    def add_new_customers(self):
        """Add new customers based on acquisition rate for current month"""
        # Determine how many new customers to add
        base_rate = self.acquisition_rates[self.current_date.month]
        
        # Add randomness
        new_customer_count = max(1, int(np.random.poisson(base_rate)))
        
        # Generate new customer data
        new_customers = generate_customers(new_customer_count)
        
        # Adjust registration date to current date
        for customer in new_customers:
            customer['registration_date'] = self.current_date.strftime('%Y-%m-%d %H:%M:%S')
            
        # Add to customer dataframe
        new_customers_df = pd.DataFrame(new_customers)
        self.customers_df = pd.concat([self.customers_df, new_customers_df], ignore_index=True)
        
    def generate_daily_orders(self):
        """Generate orders for the current date"""
        # Determine how many orders to create based on day of week and season
        # Weekends and holidays have more orders
        day_of_week = self.current_date.weekday()  # 0-6 (Mon-Sun)
        month = self.current_date.month
        
        # Base order count varies by day of week (higher on weekends)
        base_order_count = 8 if day_of_week >= 5 else 5  # Weekend vs weekday
        
        # Seasonal adjustments
        if month == 4:  # Ramadan
            seasonal_multiplier = 1.5
        elif month == 9:  # Tabaski
            seasonal_multiplier = 1.8
        elif month == 12:  # End of year holidays
            seasonal_multiplier = 2.0
        else:
            seasonal_multiplier = 1.0
            
        # Final count with some randomness
        order_count = max(1, int(np.random.poisson(base_order_count * seasonal_multiplier)))
        
        # Generate orders using the function that takes a specific date
        today_orders = generate_orders_for_date(
            self.customers_df,
            order_count,
            self.current_date
        )
        
        # Update customer data based on orders
        for order in today_orders:
            customer_id = order['customer_id']
            customer_idx = self.customers_df[self.customers_df['customer_id'] == customer_id].index
            
            if len(customer_idx) > 0:
                idx = customer_idx[0]
                
                # Update last purchase date
                self.customers_df.at[idx, 'last_purchase_date'] = order['order_date']
                
                # If this is their first order, update first purchase date
                if pd.isna(self.customers_df.at[idx, 'first_purchase_date']):
                    self.customers_df.at[idx, 'first_purchase_date'] = order['order_date']
                
                # Increment order count
                self.customers_df.at[idx, 'total_orders'] += 1
                
                # Update lifetime value
                prev_value = self.customers_df.at[idx, 'lifetime_value']
                if pd.isna(prev_value):
                    prev_value = 0
                self.customers_df.at[idx, 'lifetime_value'] = prev_value + order['final_total']
        
        # Add new orders to order database
        if today_orders:
            today_orders_df = pd.DataFrame([{**order, 'items': str(order['items'])} for order in today_orders])
            self.orders_df = pd.concat([self.orders_df, today_orders_df], ignore_index=True)


# Main execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate CRM data with time progression')
    parser.add_argument('--start-date', default='2025-01-01', help='Start date for data generation (YYYY-MM-DD)')
    parser.add_argument('--end-date', default='2025-03-28', help='End date for data generation (YYYY-MM-DD)')
    parser.add_argument('--frequency', choices=['daily', 'weekly', 'monthly'], default='daily', 
                      help='Frequency of data snapshots')
    parser.add_argument('--initial-customers', type=int, default=500, help='Number of initial customers')
    
    args = parser.parse_args()
    
    generator = CRMDataGenerator(
        start_date=args.start_date,
        end_date=args.end_date,
        snapshot_frequency=args.frequency
    )
    
    generator.initialize_data(num_customers=args.initial_customers)
    generator.generate_time_series()
    
    print("CRM data generation complete!")