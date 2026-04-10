import pandas as pd
from src.database.connection import get_db_connection

def get_dynamic_thresholds(engine, area, years_back=2):
    """
    Dynamically calculates Q1 (Best 25%) and Q3 (Worst 25%) 
    for Price and CO2 over the specified historical period.
    """
    print(f"🧮 Calculating dynamic {years_back}-year statistical standards for {area}...")
    query = f"""
        SELECT spot_price_dkk_kwh, co2_emissions_g_kwh 
        FROM historical_training_data 
        WHERE price_area = '{area}' 
        AND ds >= NOW() - INTERVAL '{years_back} YEARS'
    """
    df = pd.read_sql(query, engine)
    
    return {
        'q1_price': df['spot_price_dkk_kwh'].quantile(0.33),
        'q3_price': df['spot_price_dkk_kwh'].quantile(0.83),
        'q1_co2': df['co2_emissions_g_kwh'].quantile(0.33),
        'q3_co2': df['co2_emissions_g_kwh'].quantile(0.83)
    }

def apply_regional_logic(row, thresholds_dk1, thresholds_dk2):
    """
    Evaluates the prediction against the living 2-year statistics.
    🟢 BEST: Top 33% cleanest & cheapest | 🔴 AVOID: Worst 15% or Peak Hours.
    """
    area, hour = row['price_area'], row['forecast_time'].hour
    price, co2 = row['spot_price_dkk_kwh'], row['predicted_co2']

    # Select the correct dynamic dictionary based on the row's area
    t = thresholds_dk1 if area == 'DK1' else thresholds_dk2

    # Peak hour penalty (17:00 to 21:00 is universally bad for grid load)
    if (17 <= hour <= 21) or (price > t['q3_price']) or (co2 > t['q3_co2']): 
        return "🔴 AVOID "
    elif (price < t['q1_price']) and (co2 < t['q1_co2']): 
        return "🟢 BEST  "
    else: 
        return "🟡 CAUTION"

def generate_manifesto(df):
    """Generates the universal text manifesto based on the dynamic results."""
    green_hours = len(df[df['status'].str.contains("🟢")])
    
    legend = (
        "🟢 BEST: A gift from the wind and sun. Pure harmony for the planet.\n"
        "🟡 CAUTION: The grid is stable, but your mindfulness is still needed.\n"
        "🔴 AVOID: High tariffs or carbon. Protecting our neighbors and the elnet.\n\n"
        "🟢 BEST: En gave fra vind og sol. Ren harmoni for vores planet.\n"
        "🟡 CAUTION: Elnettet er stabilt, men din opmærksomhed er stadig vigtig.\n"
        "🔴 AVOID: Høje tariffer eller CO2. Vi beskytter vores naboer og elnettet.\n"
    )

    if green_hours > 0:
        en_strat = f"The grid offers {green_hours} hours of 'Best' energy today; let's use this wind-powered gift to heal our home."
        da_strat = f"Elnettet tilbyder {green_hours} 'Best' timer i dag; lad os bruge denne vinddrevne gave til at passe på vores hjem."
    else:
        en_strat = "Today, the winds are quiet and the planet is resting. We choose patience over consumption, waiting for the green light to return."
        da_strat = "I dag er vinden stille, og planeten hviler. Vi vælger tålmodighed frem for forbrug og venter på, at det grønne lys vender tilbage."

    return f"{legend}\n---\nEN: {en_strat}\nDA: {da_strat}"

def run_recommendation_engine():
    engine = get_db_connection()
    
    # 1. Fetch dynamic statistics for the last 2 years
    dk1_thresholds = get_dynamic_thresholds(engine, 'DK1', years_back=2)
    dk2_thresholds = get_dynamic_thresholds(engine, 'DK2', years_back=2)
    
    # 2. Fetch the 48 newest predictions
    query = "SELECT * FROM forecast_results ORDER BY generated_at DESC LIMIT 48"
    df = pd.read_sql(query, engine)
    
    # 3. Apply the dynamic logic to the dataframe
    df['forecast_time'] = pd.to_datetime(df['forecast_time'])
    df['status'] = df.apply(lambda row: apply_regional_logic(row, dk1_thresholds, dk2_thresholds), axis=1)
    df['Time'] = df['forecast_time'].dt.strftime('%H:%M')

    # --- PRINTING THE RESULTS ---
    print("\n" + "🌍 " * 15)
    print("      GRIDWISE: THE DATA-DRIVEN TRUTH")
    print("🌍 " * 15)

    for area in ['DK1', 'DK2']:
        name = "Aalborg / Jutland" if area == 'DK1' else "Copenhagen / Zealand"
        print(f"\n📊 {area} - {name}")
        print("-" * 60)
        print(f"{'TIME':<8} | {'PRICE':<9} | {'CO2':<8} | {'STATUS'}")
        
        area_df = df[df['price_area'] == area].sort_values('Time')
        for _, r in area_df.iterrows():
            print(f"{r['Time']:<8} | {r['spot_price_dkk_kwh']:>6.2f} kr | {r['predicted_co2']:>5.1f}g | {r['status']}")

    print("\n" + "🌱 " * 15)
    print("🌍 THE GUARDIAN'S WORD / VOGTERENS ORD")
    print("🌱 " * 15)
    print(generate_manifesto(df))
    print("-" * 60)

if __name__ == "__main__":
    run_recommendation_engine()