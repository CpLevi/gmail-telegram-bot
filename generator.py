"""
EarnX Gmail Bot — Name / Email / Password Generator
Generates realistic Indian + US mixed names (80% male, 20% female),
clean emails with minimal numbers, and strong passwords.
"""

import random
import string
import logging
from datetime import datetime

from database import get_db

logger = logging.getLogger(__name__)

# ==================== NAME DATABASE — INDIAN + US MIXED ====================

# ─── MALE FIRST NAMES (Indian ~60%, US ~40%) ───
MALE_FIRST_NAMES = [
    # Indian names
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
    # US names
    "James", "John", "Robert", "Michael", "David", "William", "Richard", "Joseph", "Thomas", "Charles",
    "Daniel", "Matthew", "Anthony", "Mark", "Steven", "Paul", "Andrew", "Joshua", "Kevin", "Brian",
    "Ryan", "Jason", "Brandon", "Justin", "Tyler", "Austin", "Nathan", "Aaron", "Jacob", "Ethan",
    "Mason", "Logan", "Lucas", "Liam", "Noah", "Oliver", "Aiden", "Elijah", "Jackson", "Carter",
    "Dylan", "Luke", "Gabriel", "Owen", "Caleb", "Connor", "Isaac", "Jayden", "Hunter", "Adrian",
    "Evan", "Ian", "Marcus", "Cole", "Derek", "Troy", "Scott", "Kyle", "Blake", "Chase",
    "Gavin", "Trevor", "Spencer", "Carl", "Alex", "Max", "Leo", "Nolan", "Miles", "Grant",
    "Dean", "Eric", "Sean", "Patrick", "Victor", "Ray", "Craig", "Keith", "Roger", "Frank",
]

# ─── FEMALE FIRST NAMES (Indian ~60%, US ~40%) ───
FEMALE_FIRST_NAMES = [
    # Indian names
    "Aanya", "Aditi", "Aisha", "Ananya", "Anjali", "Anita", "Ankita", "Aparna", "Archana", "Bhavna",
    "Chitra", "Deepa", "Diya", "Divya", "Esha", "Garima", "Hema", "Isha", "Jaya", "Jyoti",
    "Kajal", "Kavita", "Kavya", "Kiran", "Komal", "Lakshmi", "Lata", "Madhu", "Mansi", "Maya",
    "Meena", "Megha", "Nandini", "Neha", "Nidhi", "Nikita", "Nisha", "Pallavi", "Payal", "Pooja",
    "Prachi", "Pragya", "Preeti", "Prisha", "Priya", "Radha", "Ragini", "Rani", "Rashmi", "Rekha",
    "Riya", "Roshni", "Sakshi", "Sandhya", "Sara", "Seema", "Shikha", "Shivani", "Shreya", "Simran",
    "Sneha", "Sonali", "Sonia", "Swati", "Tanvi", "Tara", "Trisha", "Vaishali", "Vandana", "Varsha",
    "Aarohi", "Kiara", "Myra", "Saanvi", "Aadya", "Ira", "Navya", "Pihu", "Siya", "Avni",
    # US names
    "Emily", "Sarah", "Jessica", "Ashley", "Amanda", "Jennifer", "Lauren", "Megan", "Samantha", "Rachel",
    "Nicole", "Hannah", "Brittany", "Kayla", "Olivia", "Emma", "Sophia", "Ava", "Isabella", "Mia",
    "Chloe", "Grace", "Lily", "Ella", "Zoe", "Madison", "Abigail", "Natalie", "Victoria", "Hazel",
    "Riley", "Nora", "Stella", "Lucy", "Aria", "Scarlett", "Claire", "Leah", "Brooke", "Morgan",
    "Taylor", "Tiffany", "Amber", "Crystal", "Heather", "Kelly", "Vanessa", "Courtney", "Dana", "Paige",
]

# ─── LAST NAMES (Indian ~60%, US ~40%) ───
LAST_NAMES = [
    # Indian surnames
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
    # US/Western surnames
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
    "Wilson", "Anderson", "Taylor", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Moore",
    "Clark", "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott", "Green",
    "Baker", "Adams", "Nelson", "Hill", "Campbell", "Mitchell", "Roberts", "Carter", "Phillips", "Evans",
    "Turner", "Parker", "Collins", "Edwards", "Stewart", "Morris", "Reed", "Cooper", "Morgan", "Bennett",
    "Barnes", "Fisher", "Henderson", "Brooks", "Ross", "Hamilton", "Graham", "Price", "Fox", "West",
]


# ==================== GENERATION FUNCTIONS ====================

def _pick_gender():
    """80% male, 20% female."""
    return "M" if random.random() < 0.80 else "F"


def _generate_email_username(first_name: str, last_name: str) -> str:
    """
    Generate a realistic, clean email username.
    Only 0-2 numbers to keep it creatable (not already taken on Gmail).
    Uses realistic patterns real people actually use.
    """
    fn = first_name.lower()
    ln = last_name.lower()

    # Single digit (0-9) or two digits (10-99) — keeps it clean
    d1 = str(random.randint(1, 9))
    d2 = str(random.randint(10, 99))

    # Weighted patterns — most common real-world formats
    patterns = [
        # With 1 number (most common)
        f"{fn}.{ln}{d1}",               # john.smith7
        f"{fn}{ln}{d1}",                # johnsmith3
        f"{fn}.{d1}{ln}",               # john.5smith
        f"{fn}{d1}.{ln}",               # john3.smith
        f"{fn}{d1}{ln}",                # john5smith

        # With 2 numbers
        f"{fn}.{ln}{d2}",               # john.smith42
        f"{fn}{ln}{d2}",                # johnsmith85
        f"{fn}{d2}{ln}",                # john71smith

        # Zero numbers (dot separated — very clean)
        f"{fn}.{ln}",                   # john.smith
        f"{fn}{ln}",                    # johnsmith

        # Initial patterns (clean, unique)
        f"{fn[0]}.{ln}{d1}",            # j.smith4
        f"{fn}.{ln[0]}{d2}",            # john.s29
        f"{fn}{ln[0]}{d1}",             # johns5
    ]

    # Weight towards 1-number patterns (more creatable)
    weights = [
        15, 12, 10, 10, 10,    # 1-number patterns (57%)
        8, 8, 7,                # 2-number patterns (23%)
        4, 3,                   # 0-number patterns (7%)
        5, 5, 3,                # initial patterns (13%)
    ]

    return random.choices(patterns, weights=weights, k=1)[0]


def _generate_password(first_name: str, last_name: str, age: int) -> str:
    """Generate a strong but memorable password."""
    fn = first_name.capitalize()
    ln = last_name.capitalize()
    specials = ["@", "#", "$", "!", "&", "*"]
    spec = random.choice(specials)
    yr = str(datetime.now().year - age)[-2:]  # birth year last 2 digits
    num2 = str(random.randint(10, 99))
    num1 = str(random.randint(1, 9))
    letters = ''.join(random.choices(string.ascii_letters, k=2))

    patterns = [
        f"{fn}{spec}{yr}{num2}",                   # John@0347
        f"{fn[:3]}{spec}{ln[:3]}{yr}{num1}",       # Joh@Smi035
        f"{ln}{spec}{fn[:2]}{num2}",               # Smith@Jo47
        f"{fn}{spec}{num2}{letters.upper()}",      # John@47AB
        f"{fn[:4]}{ln[:2]}{spec}{yr}{num1}",       # JohnSm@034
        f"{fn}{yr}{spec}{num2}",                   # John03@47
        f"{ln[:3]}{fn[:3]}{spec}{num2}{num1}",     # SmiJoh@473
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
    Generate a single task with name, age, email, and password.
    80% male, 20% female. Age always 20+. Indian + US mixed.
    """
    max_retries = 15
    for _ in range(max_retries):
        gender = _pick_gender()
        if gender == "M":
            first_name = random.choice(MALE_FIRST_NAMES)
        else:
            first_name = random.choice(FEMALE_FIRST_NAMES)

        last_name = random.choice(LAST_NAMES)
        age = random.randint(20, 40)  # Always 20+

        email_user = _generate_email_username(first_name, last_name)
        email = f"{email_user}@gmail.com"

        # Check uniqueness
        if _is_email_taken(email):
            continue

        password = _generate_password(first_name, last_name, age)

        # Generate a unique task ID
        task_id = f"T-{random.randint(1000, 9999)}-{int(datetime.now().timestamp()) % 10000}"

        return {
            "task_id": task_id,
            "first_name": first_name,
            "last_name": last_name,
            "age": age,
            "gender": gender,
            "email": email,
            "password": password,
        }

    logger.warning(f"Failed to generate unique task after {max_retries} retries")
    return None


def generate_bulk_tasks(user_id: int, count: int) -> tuple:
    """
    Generate multiple tasks for bulk submission.
    Returns (batch_id, list_of_tasks).
    """
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
    """
    Save a generated task to the gmail table.
    Returns the gmail record ID or None on failure.
    """
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO gmail (
                    user_id, email, password, reward, submit_date,
                    status, task_id, assigned_first_name, assigned_last_name,
                    assigned_age, assigned_email, assigned_password,
                    task_status, task_assigned_at, batch_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                task["age"],
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
