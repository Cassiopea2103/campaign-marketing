import numpy as np
import uuid
import random
import datetime
from faker import Faker
import os

# Set up Faker with locale support
fake = Faker(['fr_SN', 'fr_FR'])  # Senegalese French and Standard French
Faker.seed(42)  # For reproducibility

# Create output directory if it doesn't exist
os.makedirs('data', exist_ok=True)

# Define Senegalese cities with population-based weights
cities = {
    "Dakar": 0.35, "Thiès": 0.15, "Touba": 0.12, "Rufisque": 0.08, 
    "Saint-Louis": 0.07, "Mbour": 0.05, "Kaolack": 0.05, "Ziguinchor": 0.04, 
    "Diourbel": 0.03, "Louga": 0.03, "Tambacounda": 0.03
}

# Senegalese phone number formats
def generate_senegal_phone():
    # Senegal mobile prefixes
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
        
        # Basic information
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}@{random.choice(['gmail.com', 'yahoo.fr', 'hotmail.com', 'outlook.com', 'orange.sn'])}"
        phone = generate_senegal_phone()
        
        # Location (weighted by population)
        city = random.choices(list(cities.keys()), weights=list(cities.values()))[0]
        
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
        
        # Gender (primarily women for cosmetics)
        gender = random.choices(["F", "M", "Autre"], weights=[0.8, 0.15, 0.05])[0]
        
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
            "address": fake.street_address() if has_purchased else None,
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
        if pd.notna(customer['first_purchase_date']) and pd.notna(customer['last_purchase_date']):
            order_date = fake.date_time_between(
                start_date=customer['first_purchase_date'], 
                end_date=customer['last_purchase_date']
            )
        else:
            # Fallback if dates are missing
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

# Run the generation
num_customers = 1000
num_orders = 3000

# Generate customer data
print("Generating customer data...")
customers_data = generate_customers(num_customers)
customers_df = pd.DataFrame(customers_data)

# Save customer data
customers_df.to_csv('data/customers.csv', index=False)
print(f"Saved {len(customers_df)} customer records to 'data/customers.csv'")

# Generate order data
print("Generating order data...")
orders_data = generate_orders(customers_df, num_orders)

# Convert order items to string for storage in CSV
for order in orders_data:
    order['items'] = str(order['items'])

orders_df = pd.DataFrame(orders_data)
orders_df.to_csv('data/orders.csv', index=False)
print(f"Saved {len(orders_df)} order records to 'data/orders.csv'")

# Preview the data
print("\nCustomer Data Preview:")
print(customers_df.head())

print("\nOrder Data Preview:")
print(orders_df.head())