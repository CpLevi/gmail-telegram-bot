"""
EarnX Gmail Bot — Name / Email / Password Generator
Generates realistic names (Indian OR US — never mixed), clean emails, and strong passwords.
80% male, 20% female. DOB always 21–40 years old.
"""

import random
import string
import logging
from datetime import datetime, timedelta

from database import get_db

logger = logging.getLogger(__name__)

# ==================== NAME DATABASE — SEPARATED POOLS ====================

# ─── INDIAN MALE FIRST NAMES ───
INDIAN_MALE_FIRST = [
    "Aarav", "Aditya", "Akash", "Aman", "Amit", "Anand", "Anil", "Arjun", "Ashish", "Ashok",
    "Bharat", "Chandan", "Chirag", "Deepak", "Devesh", "Dhruv", "Dinesh", "Gaurav", "Harsh", "Hemant",
    "Hitesh", "Ishaan", "Jatin", "Jayesh", "Karan", "Kartik", "Kunal", "Lalit", "Lokesh", "Manish",
    "Mayank", "Mohit", "Mukesh", "Naman", "Naveen", "Nikhil", "Nitin", "Omkar", "Pankaj", "Pawan",
    "Pradeep", "Pranav", "Pratik", "Rahul", "Rajesh", "Rakesh", "Ravi", "Ritik", "Rohan", "Rohit",
    "Sachin", "Sahil", "Sanjay", "Saurabh", "Shivam", "Shubham", "Sumit", "Sunil", "Suresh", "Tushar",
    "Varun", "Vijay", "Vikram", "Vinay", "Vishal", "Vivek", "Yash", "Abhishek", "Ajay", "Alok",
    "Ankur", "Anuj", "Brijesh", "Daksh", "Darshan", "Girish", "Gopal", "Hardik", "Harish", "Himanshu",
    "Kamal", "Kapil", "Krishna", "Madhav", "Manoj", "Neeraj", "Paras", "Piyush", "Raghav", "Rajat",
    "Ramesh", "Rupesh", "Sagar", "Sandeep", "Shreyas", "Siddharth", "Tarun", "Uday", "Utkarsh", "Yogesh",
    "Arnav", "Reyansh", "Vihaan", "Kabir", "Advait", "Rudra", "Atharv", "Tanmay", "Tejas", "Laksh",
    "Ayaan", "Dhairya", "Ishan", "Krish", "Parth", "Samar", "Ved", "Yuvraj", "Aarush", "Ankit",
    "Bhavesh", "Chiranjeev", "Dilip", "Farhan", "Ganesh", "Hari", "Jayant", "Kishore", "Lalit", "Mohan",
    "Nirav", "Prasad", "Rajan", "Sameer", "Sudhir", "Trilok", "Umang", "Venkat", "Yatin", "Zubin",
]

# ─── INDIAN FEMALE FIRST NAMES ───
INDIAN_FEMALE_FIRST = [
    "Aanya", "Aditi", "Aisha", "Ananya", "Anjali", "Anita", "Ankita", "Aparna", "Archana", "Bhavna",
    "Chitra", "Deepa", "Diya", "Divya", "Esha", "Garima", "Hema", "Isha", "Jaya", "Jyoti",
    "Kajal", "Kavita", "Kavya", "Kiran", "Komal", "Lakshmi", "Lata", "Madhu", "Mansi", "Maya",
    "Meena", "Megha", "Nandini", "Neha", "Nidhi", "Nikita", "Nisha", "Pallavi", "Payal", "Pooja",
    "Prachi", "Pragya", "Preeti", "Prisha", "Priya", "Radha", "Ragini", "Rani", "Rashmi", "Rekha",
    "Riya", "Roshni", "Sakshi", "Sandhya", "Sara", "Seema", "Shikha", "Shivani", "Shreya", "Simran",
    "Sneha", "Sonali", "Sonia", "Swati", "Tanvi", "Tara", "Trisha", "Vaishali", "Vandana", "Varsha",
    "Aarohi", "Kiara", "Myra", "Saanvi", "Aadya", "Ira", "Navya", "Pihu", "Siya", "Avni",
    "Bhoomika", "Charvi", "Damini", "Falguni", "Gauri", "Harini", "Janvi", "Kriti", "Latika", "Mitali",
    "Naina", "Parul", "Ritika", "Shalini", "Tanuja", "Urvi", "Vrinda", "Yamini", "Zara", "Anvi",
]

# ─── INDIAN LAST NAMES ───
INDIAN_LAST = [
    "Agarwal", "Arora", "Bansal", "Bhatia", "Bhatt", "Bisht", "Chauhan", "Chopra", "Choudhary", "Das",
    "Desai", "Dubey", "Garg", "Ghosh", "Goyal", "Gupta", "Iyer", "Jain", "Jha", "Joshi",
    "Kapoor", "Kaur", "Khan", "Kohli", "Kumar", "Lal", "Mahajan", "Malhotra", "Mehra", "Mehta",
    "Mishra", "Mittal", "Mukherjee", "Nair", "Negi", "Pandit", "Pandey", "Patel", "Patil", "Prasad",
    "Rai", "Rajput", "Rana", "Rao", "Rathore", "Rawat", "Roy", "Saini", "Saxena", "Sen",
    "Shah", "Sharma", "Shukla", "Singh", "Sinha", "Srivastava", "Thakur", "Tiwari", "Trivedi", "Varma",
    "Verma", "Yadav", "Acharya", "Bajaj", "Bedi", "Bhargava", "Chawla", "Deshpande", "Dutta", "Gill",
    "Grewal", "Hegde", "Khatri", "Kulkarni", "Rastogi", "Reddy", "Sethi", "Tandon", "Walia", "Oberoi",
    "Dhawan", "Bajpai", "Chandra", "Dewan", "Grover", "Kaushik", "Khanna", "Mathur", "Narayan", "Naik",
    "Pillai", "Sachdev", "Sahni", "Sodhi", "Suri", "Vohra", "Wadhwa", "Rajan", "Hora", "Sagar",
]

# ─── US MALE FIRST NAMES ───
US_MALE_FIRST = [
    "James", "John", "Robert", "Michael", "David", "William", "Richard", "Joseph", "Thomas", "Charles",
    "Daniel", "Matthew", "Anthony", "Mark", "Steven", "Paul", "Andrew", "Joshua", "Kevin", "Brian",
    "Ryan", "Jason", "Brandon", "Justin", "Tyler", "Austin", "Nathan", "Aaron", "Jacob", "Ethan",
    "Mason", "Logan", "Lucas", "Liam", "Noah", "Oliver", "Aiden", "Elijah", "Jackson", "Carter",
    "Dylan", "Luke", "Gabriel", "Owen", "Caleb", "Connor", "Isaac", "Jayden", "Hunter", "Adrian",
    "Evan", "Ian", "Marcus", "Cole", "Derek", "Troy", "Scott", "Kyle", "Blake", "Chase",
    "Gavin", "Trevor", "Spencer", "Carl", "Alex", "Max", "Leo", "Nolan", "Miles", "Grant",
    "Dean", "Eric", "Sean", "Patrick", "Victor", "Ray", "Craig", "Keith", "Roger", "Frank",
    "Brett", "Brent", "Cody", "Dustin", "Eddie", "Felix", "Greg", "Henry", "Ivan", "Jack",
    "Kent", "Larry", "Mike", "Neil", "Oscar", "Pete", "Quinn", "Ross", "Steve", "Todd",
    "Vince", "Wade", "Xavier", "Zach", "Cameron", "Wesley", "Brody", "Carson", "Cooper", "Hudson",
    "Wyatt", "Colton", "Tanner", "Dalton", "Landon", "Travis", "Mitchell", "Kendrick", "Donovan", "Riley",
]

# ─── US FEMALE FIRST NAMES ───
US_FEMALE_FIRST = [
    "Emily", "Sarah", "Jessica", "Ashley", "Amanda", "Jennifer", "Lauren", "Megan", "Samantha", "Rachel",
    "Nicole", "Hannah", "Brittany", "Kayla", "Olivia", "Emma", "Sophia", "Ava", "Isabella", "Mia",
    "Chloe", "Grace", "Lily", "Ella", "Zoe", "Madison", "Abigail", "Natalie", "Victoria", "Hazel",
    "Riley", "Nora", "Stella", "Lucy", "Aria", "Scarlett", "Claire", "Leah", "Brooke", "Morgan",
    "Taylor", "Tiffany", "Amber", "Crystal", "Heather", "Kelly", "Vanessa", "Courtney", "Dana", "Paige",
    "Audrey", "Bella", "Caroline", "Daisy", "Elena", "Faith", "Gabriella", "Harper", "Iris", "Julia",
    "Katherine", "Laura", "Mackenzie", "Naomi", "Peyton", "Reagan", "Sierra", "Trinity", "Violet", "Wendy",
    "Alexis", "Bethany", "Chelsea", "Diana", "Evelyn", "Fiona", "Giselle", "Holly", "Ivy", "Jasmine",
]

# ─── US LAST NAMES ───
US_LAST = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
    "Wilson", "Anderson", "Taylor", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Moore",
    "Clark", "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott", "Green",
    "Baker", "Adams", "Nelson", "Hill", "Campbell", "Mitchell", "Roberts", "Carter", "Phillips", "Evans",
    "Turner", "Parker", "Collins", "Edwards", "Stewart", "Morris", "Reed", "Cooper", "Morgan", "Bennett",
    "Barnes", "Fisher", "Henderson", "Brooks", "Ross", "Hamilton", "Graham", "Price", "Fox", "West",
    "Sullivan", "Russell", "Wood", "Coleman", "Hayes", "Murphy", "Rivera", "Sanders", "Patterson", "Long",
    "Ford", "Butler", "Warren", "Gibson", "Spencer", "Gordon", "Wells", "Fox", "Marshall", "Hunt",
    "Stone", "Grant", "Hudson", "Webb", "Crawford", "Burns", "Palmer", "Day", "Riley", "Owens",
    "Lane", "Burke", "Ray", "Cole", "Walsh", "Hart", "Duncan", "Pierce", "Floyd", "Carr",
]


# ==================== GENERATION FUNCTIONS ====================

def _pick_gender():
    """80% male, 20% female."""
    return "M" if random.random() < 0.80 else "F"


def _pick_origin():
    """50% Indian, 50% US."""
    return "indian" if random.random() < 0.50 else "us"


def _pick_name(gender, origin):
    """Pick first+last name from same origin pool. Never mix."""
    if origin == "indian":
        first = random.choice(INDIAN_MALE_FIRST if gender == "M" else INDIAN_FEMALE_FIRST)
        last = random.choice(INDIAN_LAST)
    else:
        first = random.choice(US_MALE_FIRST if gender == "M" else US_FEMALE_FIRST)
        last = random.choice(US_LAST)
    return first, last


def _generate_dob(min_age=21, max_age=40):
    """Generate a random DOB between min_age and max_age years ago.
    Returns (dob_string, birth_year)."""
    today = datetime.now()
    age = random.randint(min_age, max_age)
    birth_year = today.year - age
    birth_month = random.randint(1, 12)
    # Safe day range for the month
    if birth_month in (1, 3, 5, 7, 8, 10, 12):
        max_day = 28  # keep safe
    elif birth_month == 2:
        max_day = 28
    else:
        max_day = 28
    birth_day = random.randint(1, max_day)

    dob_date = datetime(birth_year, birth_month, birth_day)
    # Format: "March 15, 1998"
    dob_str = dob_date.strftime("%B %d, %Y")
    return dob_str, birth_year


def _generate_email_username(first_name: str, last_name: str) -> str:
    """
    Generate a realistic, clean email username.
    Only 0-2 numbers to keep it creatable.
    """
    fn = first_name.lower()
    ln = last_name.lower()

    d1 = str(random.randint(1, 9))
    d2 = str(random.randint(10, 99))

    patterns = [
        # With 1 number (most common)
        f"{fn}.{ln}{d1}",
        f"{fn}{ln}{d1}",
        f"{fn}.{d1}{ln}",
        f"{fn}{d1}.{ln}",
        f"{fn}{d1}{ln}",

        # With 2 numbers
        f"{fn}.{ln}{d2}",
        f"{fn}{ln}{d2}",
        f"{fn}{d2}{ln}",

        # Zero numbers
        f"{fn}.{ln}",
        f"{fn}{ln}",

        # Initial patterns
        f"{fn[0]}.{ln}{d1}",
        f"{fn}.{ln[0]}{d2}",
        f"{fn}{ln[0]}{d1}",
    ]

    weights = [
        15, 12, 10, 10, 10,    # 1-number (57%)
        8, 8, 7,                # 2-number (23%)
        4, 3,                   # 0-number (7%)
        5, 5, 3,                # initial (13%)
    ]

    return random.choices(patterns, weights=weights, k=1)[0]


def _generate_password(first_name: str, last_name: str, birth_year: int) -> str:
    """Generate a strong but memorable password."""
    fn = first_name.capitalize()
    ln = last_name.capitalize()
    specials = ["@", "#", "$", "!", "&", "*"]
    spec = random.choice(specials)
    yr = str(birth_year)[-2:]
    num2 = str(random.randint(10, 99))
    num1 = str(random.randint(1, 9))
    letters = ''.join(random.choices(string.ascii_letters, k=2))

    patterns = [
        f"{fn}{spec}{yr}{num2}",
        f"{fn[:3]}{spec}{ln[:3]}{yr}{num1}",
        f"{ln}{spec}{fn[:2]}{num2}",
        f"{fn}{spec}{num2}{letters.upper()}",
        f"{fn[:4]}{ln[:2]}{spec}{yr}{num1}",
        f"{fn}{yr}{spec}{num2}",
        f"{ln[:3]}{fn[:3]}{spec}{num2}{num1}",
    ]

    return random.choice(patterns)


def _is_email_taken(email: str) -> bool:
    """Check if an email already exists in the database."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT id FROM gmail WHERE LOWER(email) = %s LIMIT 1", (email.lower(),))
            return c.fetchone() is not None
    except Exception:
        return False


def generate_single_task(user_id: int) -> dict:
    """
    Generate a single task with name, DOB, email, and password.
    80% male / 20% female. Indian OR US names (never mixed). DOB 21-40.
    """
    max_retries = 15
    for _ in range(max_retries):
        gender = _pick_gender()
        origin = _pick_origin()
        first_name, last_name = _pick_name(gender, origin)

        dob_str, birth_year = _generate_dob(21, 40)

        email_user = _generate_email_username(first_name, last_name)
        email = f"{email_user}@gmail.com"

        if _is_email_taken(email):
            continue

        password = _generate_password(first_name, last_name, birth_year)

        task_id = f"T-{random.randint(1000, 9999)}-{int(datetime.now().timestamp()) % 10000}"

        return {
            "task_id": task_id,
            "first_name": first_name,
            "last_name": last_name,
            "dob": dob_str,
            "gender": gender,
            "email": email,
            "password": password,
        }

    logger.warning(f"Failed to generate unique task after {max_retries} retries")
    return None


def generate_bulk_tasks(user_id: int, count: int) -> tuple:
    """Generate multiple tasks for bulk submission."""
    batch_id = f"B-{random.randint(1000, 9999)}"
    tasks = []
    generated_emails = set()

    for _ in range(count):
        max_retries = 15
        for __ in range(max_retries):
            task = generate_single_task(user_id)
            if task and task["email"] not in generated_emails:
                task["batch_id"] = batch_id
                generated_emails.add(task["email"])
                tasks.append(task)
                break

    return batch_id, tasks


def save_task_to_db(user_id: int, task: dict, reward) -> int | None:
    """Save a generated task to the gmail table."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO gmail (
                    user_id, email, password, reward, submit_date,
                    status, task_id, assigned_first_name, assigned_last_name,
                    assigned_dob, assigned_gender, assigned_email, assigned_password,
                    task_status, task_assigned_at, batch_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                user_id,
                task["email"],
                task["password"],
                reward,
                datetime.now().isoformat(),
                "pending",
                task["task_id"],
                task["first_name"],
                task["last_name"],
                task["dob"],
                task["gender"],
                task["email"],
                task["password"],
                "assigned",
                datetime.now().isoformat(),
                task.get("batch_id"),
            ))
            result = c.fetchone()
            return result['id'] if result else None
    except Exception as e:
        logger.error(f"Error saving task to DB: {e}")
        return None


def confirm_task(task_id: str) -> bool:
    """Mark a task as confirmed by the user."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""
                UPDATE gmail
                SET task_status = 'confirmed', task_confirmed_at = %s
                WHERE task_id = %s AND task_status = 'assigned'
                RETURNING id
            """, (datetime.now().isoformat(), task_id))
            return c.fetchone() is not None
    except Exception as e:
        logger.error(f"Error confirming task {task_id}: {e}")
        return False


def skip_task(task_id: str) -> bool:
    """Delete a skipped task (before user created the account)."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""
                DELETE FROM gmail
                WHERE task_id = %s AND task_status = 'assigned'
                RETURNING id
            """, (task_id,))
            result = c.fetchone()
            if result:
                c.execute("""
                    UPDATE users SET total_gmail = GREATEST(total_gmail - 1, 0)
                    WHERE user_id = (SELECT user_id FROM gmail WHERE task_id = %s)
                """, (task_id,))
            return result is not None
    except Exception as e:
        logger.error(f"Error skipping task {task_id}: {e}")
        return False
