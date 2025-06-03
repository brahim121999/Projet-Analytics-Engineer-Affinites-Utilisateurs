import duckdb

# Connexion à la base DuckDB créée par DBT
con = duckdb.connect("veepee_ads.db")

# 1. Affinités utilisateurs par thématique
df1 = con.execute("SELECT * FROM main_analytics.user_affinities LIMIT 10").df()
print("Affinités utilisateurs (par thématique) :")
print(df1)

# 2. Thèmes des campagnes
df2 = con.execute("""
    SELECT sector, sub_sector, theme, COUNT(*) 
    FROM main_intermediate.int_campaigns_with_themes 
    GROUP BY 1, 2, 3
""").df()
print("\nThèmes des campagnes :")
print(df2)

# 3. Événements utilisateur (visites et commandes)
df3 = con.execute("""
    SELECT user_id, event_type, COUNT(*) 
    FROM main_intermediate.int_user_events 
    GROUP BY 1, 2
""").df()
print("\nÉvénements utilisateur :")
print(df3)

# 4. Nouvelle table silver : affinités par combinaison complète
df4 = con.execute("""
    SELECT * 
    FROM main_analytics.user_affinity_levels 
    ORDER BY total_events DESC 
    LIMIT 10
""").df()
print("\nTop affinités utilisateur (niveau complet) :")
print(df4)


con.close()
