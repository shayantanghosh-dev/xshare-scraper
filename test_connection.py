from utils.supabase_client import supabase

result = supabase.table("hackathons").select("*").execute()
print("Connection successful!")
print(f"Records in table: {len(result.data)}")