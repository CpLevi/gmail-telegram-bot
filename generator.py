"""
EarnX Gmail Bot — Name / Email / Password Generator
Generates realistic names from multiple countries — 90% international / 10% Indian.
70% male, 30% female. DOB always 21–40 years old.

NAME SYSTEM: Curated real names from 7 countries (US, UK, France, Germany, Spain, Italy, Australia + India).
EMAIL SYSTEM: Algorithmic generation with random letter+digit codes = MILLIONS of combos per name.
"""

import random
import string
import logging
from datetime import datetime, timedelta

from database import get_db

logger = logging.getLogger(__name__)

# ==================== NAME DATABASE — SEPARATED POOLS ====================

# ─── INDIAN MALE FIRST NAMES (200+) ───
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
    "Bhavesh", "Chiranjeev", "Dilip", "Farhan", "Ganesh", "Hari", "Jayant", "Kishore", "Mohan", "Prasad",
    "Nirav", "Rajan", "Sameer", "Sudhir", "Trilok", "Umang", "Venkat", "Yatin", "Zubin", "Sohail",
    "Irfan", "Zeeshan", "Fahad", "Imran", "Arif", "Rizwan", "Tanveer", "Faisal", "Nadeem", "Salman",
    "Vipin", "Bhushan", "Chandresh", "Dheeraj", "Eknath", "Govind", "Hansraj", "Jagdish", "Keshav", "Laxman",
    "Mithun", "Nagesh", "Onkar", "Pramod", "Rajeev", "Satish", "Taran", "Udayan", "Vimal", "Wasim",
    "Yashwant", "Balraj", "Chetan", "Deepesh", "Gagan", "Harjot", "Inderjit", "Jaspal", "Kuldeep", "Lovish",
    "Manpreet", "Narayan", "Omprakash", "Prashant", "Ranbir", "Surinder", "Tejpal", "Vikrant", "Ashwin", "Bhavin",
    "Darshit", "Gaurang", "Hiren", "Jigar", "Keyur", "Mitesh", "Nishant", "Paresh", "Ruchit", "Sanjeet",
]

# ─── INDIAN FEMALE FIRST NAMES (150+) ───
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
    "Aanchal", "Barkha", "Chhavi", "Devika", "Ekta", "Geeta", "Heena", "Indu", "Juhi", "Kamini",
    "Laxmi", "Mala", "Namrata", "Prerna", "Rachna", "Sapna", "Teena", "Uma", "Vidya", "Wafa",
    "Yasmin", "Zeenat", "Amrita", "Bindiya", "Champa", "Dulari", "Guddi", "Hansa", "Jhanvi", "Kanak",
    "Madhuri", "Nirmal", "Padma", "Renu", "Shobha", "Tulsi", "Usha", "Veena", "Yashi", "Alka",
    "Bhagwati", "Chameli", "Durga", "Girija", "Himani", "Jigna", "Kusum", "Manju", "Nirmala", "Pushpa",
]

# ─── INDIAN LAST NAMES (150+) ───
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
    "Ahuja", "Bakshi", "Bhalla", "Chugh", "Dang", "Goel", "Gulati", "Juneja", "Kalra", "Luthra",
    "Madan", "Nagpal", "Puri", "Sabharwal", "Talwar", "Uppal", "Vashisht", "Wahi", "Anand", "Batra",
    "Chadha", "Dhingra", "Gujral", "Handa", "Jaggi", "Kakkar", "Manchanda", "Narula", "Pahwa", "Sachdeva",
    "Trehan", "Behl", "Chhabra", "Duggal", "Kapahi", "Monga", "Pasricha", "Rekhi", "Sehgal", "Waraich",
    "Bhasin", "Chaudhuri", "Deol", "Ghai", "Johar", "Kochhar", "Mannan", "Randhawa", "Sandhu", "Sidhu",
]

# ─── US MALE FIRST NAMES (200+) ───
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
    "Ashton", "Bennett", "Calvin", "Dominic", "Emerson", "Finn", "Greyson", "Holden", "Isaiah", "Jace",
    "Kai", "Lawrence", "Maddox", "Nathaniel", "Orlando", "Preston", "Remington", "Silas", "Tristan", "Uriel",
    "Vincent", "Walter", "Xander", "Yusuf", "Zander", "Abel", "Brooks", "Clayton", "Damon", "Elliott",
    "Floyd", "Graham", "Hector", "Jared", "Kenneth", "Lincoln", "Marshall", "Newton", "Omar", "Porter",
    "Quincy", "Reese", "Sullivan", "Terrence", "Ulysses", "Vernon", "Winston", "Alvin", "Bernard", "Clifford",
    "Dennis", "Edgar", "Frederick", "Gerald", "Harold", "Jerome", "Kirk", "Leonard", "Morris", "Norman",
    "Percy", "Randall", "Sherman", "Theodore", "Warren", "Albert", "Bruce", "Cedric", "Darren", "Ernest",
    "Franklin", "Gilbert", "Harvey", "Irving", "Julius", "Karl", "Lewis", "Melvin", "Nelson", "Russell",
]

# ─── US FEMALE FIRST NAMES (150+) ───
US_FEMALE_FIRST = [
    "Emily", "Sarah", "Jessica", "Ashley", "Amanda", "Jennifer", "Lauren", "Megan", "Samantha", "Rachel",
    "Nicole", "Hannah", "Brittany", "Kayla", "Olivia", "Emma", "Sophia", "Ava", "Isabella", "Mia",
    "Chloe", "Grace", "Lily", "Ella", "Zoe", "Madison", "Abigail", "Natalie", "Victoria", "Hazel",
    "Riley", "Nora", "Stella", "Lucy", "Aria", "Scarlett", "Claire", "Leah", "Brooke", "Morgan",
    "Taylor", "Tiffany", "Amber", "Crystal", "Heather", "Kelly", "Vanessa", "Courtney", "Dana", "Paige",
    "Audrey", "Bella", "Caroline", "Daisy", "Elena", "Faith", "Gabriella", "Harper", "Iris", "Julia",
    "Katherine", "Laura", "Mackenzie", "Naomi", "Peyton", "Reagan", "Sierra", "Trinity", "Violet", "Wendy",
    "Alexis", "Bethany", "Chelsea", "Diana", "Evelyn", "Fiona", "Giselle", "Holly", "Ivy", "Jasmine",
    "Kendra", "Lindsey", "Marissa", "Nina", "Ophelia", "Penelope", "Quinn", "Rebecca", "Shelby", "Tessa",
    "Una", "Valerie", "Whitney", "Ximena", "Yolanda", "Zelda", "Addison", "Brianna", "Carmen", "Destiny",
    "Elise", "Francesca", "Gloria", "Harmony", "Imogen", "Josephine", "Kaitlyn", "Lydia", "Miranda", "Noelle",
    "Olive", "Priscilla", "Rosemary", "Sadie", "Tabitha", "Ursula", "Vera", "Willa", "Annabelle", "Beatrice",
    "Celeste", "Dorothy", "Estelle", "Florence", "Genevieve", "Harriet", "Ingrid", "Jacqueline", "Kathleen", "Louise",
    "Margot", "Nadine", "Pauline", "Ramona", "Sylvia", "Teresa", "Virginia", "Winona", "Adelaide", "Bernadette",
    "Constance", "Delilah", "Elaine", "Felicity", "Gwendolyn", "Helena", "Irene", "June", "Lillian", "Madeleine",
]

# ─── US LAST NAMES (150+) ───
US_LAST = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
    "Wilson", "Anderson", "Taylor", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Moore",
    "Clark", "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott", "Green",
    "Baker", "Adams", "Nelson", "Hill", "Campbell", "Mitchell", "Roberts", "Carter", "Phillips", "Evans",
    "Turner", "Parker", "Collins", "Edwards", "Stewart", "Morris", "Reed", "Cooper", "Morgan", "Bennett",
    "Barnes", "Fisher", "Henderson", "Brooks", "Ross", "Hamilton", "Graham", "Price", "Fox", "West",
    "Sullivan", "Russell", "Wood", "Coleman", "Hayes", "Murphy", "Rivera", "Sanders", "Patterson", "Long",
    "Ford", "Butler", "Warren", "Gibson", "Spencer", "Gordon", "Wells", "Marshall", "Hunt", "Stone",
    "Grant", "Hudson", "Webb", "Crawford", "Burns", "Palmer", "Day", "Riley", "Owens", "Lane",
    "Burke", "Ray", "Cole", "Walsh", "Hart", "Duncan", "Pierce", "Floyd", "Carr", "Daniels",
    "Chambers", "Doyle", "Keller", "Perkins", "Holland", "Johnston", "Payne", "Bates", "Schultz", "Drake",
    "Higgins", "Malone", "Maxwell", "Norris", "Pearson", "Quinn", "Reeves", "Summers", "Terry", "Vaughn",
    "Bradley", "Cross", "Duke", "Fitzgerald", "Harmon", "Jennings", "Lawson", "Mercer", "Nichols", "Olson",
    "Poole", "Ramos", "Sharp", "Townsend", "Underwood", "Wagner", "York", "Abbott", "Bowen", "Chapman",
    "Dawson", "Ellison", "Farrell", "Gentry", "Holt", "Keith", "Lambert", "McBride", "Neal", "Ortiz",
]

# ─── UK MALE FIRST NAMES (80+) ───
UK_MALE_FIRST = [
    "Oliver", "George", "Arthur", "Harry", "Jack", "Charlie", "Leo", "Oscar", "Freddie", "Archie",
    "Alfie", "Thomas", "Edward", "Henry", "Jacob", "Theo", "Noah", "Finley", "William", "Ethan",
    "Sebastian", "Rupert", "Hugo", "Felix", "Jasper", "Callum", "Liam", "Rory", "Miles", "Elliott",
    "Harvey", "Toby", "Harrison", "Angus", "Fergus", "Hamish", "Duncan", "Blair", "Alistair", "Reginald",
    "Nigel", "Colin", "Clive", "Graham", "Trevor", "Derek", "Stuart", "Keith", "Neville", "Cedric",
    "Barnaby", "Benedict", "Edmund", "Gilbert", "Leopold", "Montague", "Percival", "Quentin", "Roderick", "Winston",
    "Ewan", "Fraser", "Lachlan", "Magnus", "Rowan", "Declan", "Cillian", "Niall", "Padraig", "Rhys",
    "Gareth", "Ieuan", "Bryn", "Idris", "Gethin", "Milo", "Albie", "Reggie", "Teddy", "Louie",
]

# ─── UK FEMALE FIRST NAMES (80+) ───
UK_FEMALE_FIRST = [
    "Olivia", "Amelia", "Isla", "Ava", "Emily", "Mia", "Sophia", "Grace", "Lily", "Freya",
    "Poppy", "Daisy", "Rosie", "Florence", "Willow", "Ivy", "Elsie", "Evie", "Sienna", "Phoebe",
    "Harriet", "Imogen", "Matilda", "Amelie", "Millie", "Eliza", "Martha", "Thea", "Alice", "Beatrice",
    "Pippa", "Penelope", "Fiona", "Gemma", "Nicola", "Victoria", "Charlotte", "Eleanor", "Georgina", "Philippa",
    "Arabella", "Cordelia", "Henrietta", "Jemima", "Lavinia", "Octavia", "Prudence", "Tabitha", "Winifred", "Clementine",
    "Saoirse", "Niamh", "Aoife", "Siobhan", "Catriona", "Morag", "Eilidh", "Rhiannon", "Bronwen", "Cerys",
    "Maisie", "Molly", "Ruby", "Scarlett", "Lottie", "Ada", "Iris", "Esme", "Luna", "Cora",
    "Edith", "Agnes", "Mabel", "Nora", "Vera", "Hazel", "Pearl", "Daphne", "Margot", "Eloise",
]

# ─── UK LAST NAMES (80+) ───
UK_LAST = [
    "Smith", "Jones", "Williams", "Taylor", "Brown", "Davies", "Wilson", "Evans", "Thomas", "Johnson",
    "Roberts", "Walker", "Wright", "Robinson", "Thompson", "White", "Hughes", "Edwards", "Green", "Hall",
    "Lewis", "Harris", "Clarke", "Patel", "Jackson", "Wood", "Turner", "Martin", "Cooper", "Hill",
    "Ward", "Morris", "Moore", "Clark", "Lee", "King", "Baker", "Harrison", "Morgan", "Allen",
    "Griffiths", "Jenkins", "Owen", "Price", "Lloyd", "Rees", "Vaughan", "Watkins", "Bowen", "Parry",
    "Campbell", "Murray", "Stewart", "Watson", "Ross", "Fraser", "Hamilton", "Gray", "MacDonald", "Scott",
    "Ferguson", "Reid", "McKenzie", "Sinclair", "Henderson", "Robertson", "Wallace", "Burns", "Kerr", "Crawford",
    "OBrien", "OConnor", "Murphy", "Kelly", "Sullivan", "Lynch", "Brennan", "Gallagher", "Quinn", "Byrne",
]

# ─── FRENCH MALE FIRST NAMES (80+) ───
FRENCH_MALE_FIRST = [
    "Lucas", "Gabriel", "Leo", "Raphael", "Arthur", "Louis", "Jules", "Adam", "Hugo", "Liam",
    "Nathan", "Ethan", "Paul", "Noel", "Theo", "Sacha", "Tom", "Noah", "Enzo", "Mathis",
    "Alexandre", "Antoine", "Baptiste", "Clement", "Damien", "Emile", "Fabien", "Gregoire", "Henri", "Jacques",
    "Laurent", "Marc", "Nicolas", "Olivier", "Pierre", "Romain", "Sebastien", "Thierry", "Vincent", "Xavier",
    "Yves", "Alain", "Bernard", "Charles", "Denis", "Francois", "Gerard", "Jean", "Michel", "Philippe",
    "Maxime", "Julien", "Thomas", "Quentin", "Valentin", "Adrien", "Bastien", "Cedric", "Florian", "Guillaume",
    "Mathieu", "Thibault", "Tristan", "Arnaud", "Benoit", "Christophe", "Edouard", "Felix", "Gaspard", "Leopold",
    "Marius", "Remi", "Victor", "Axel", "Dylan", "Evan", "Gabin", "Ilyes", "Kilian", "Malo",
]

# ─── FRENCH FEMALE FIRST NAMES (80+) ───
FRENCH_FEMALE_FIRST = [
    "Emma", "Jade", "Louise", "Alice", "Chloe", "Lina", "Lea", "Rose", "Anna", "Mila",
    "Julia", "Manon", "Camille", "Ines", "Sarah", "Eva", "Zoe", "Lucie", "Clara", "Marie",
    "Amelie", "Aurelie", "Brigitte", "Caroline", "Delphine", "Eloise", "Florence", "Genevieve", "Helene", "Isabelle",
    "Juliette", "Laure", "Marguerite", "Nathalie", "Pauline", "Sandrine", "Sylvie", "Valerie", "Veronique", "Colette",
    "Adele", "Celeste", "Clemence", "Elodie", "Fleur", "Gabrielle", "Josephine", "Madeleine", "Oceane", "Solange",
    "Anais", "Aurore", "Capucine", "Estelle", "Gaelle", "Leonore", "Maelle", "Noemi", "Romane", "Victoire",
    "Charlotte", "Sophie", "Mathilde", "Margot", "Agathe", "Apolline", "Constance", "Diane", "Elise", "Lola",
    "Marion", "Nina", "Ophelie", "Penelope", "Roxane", "Daphne", "Iris", "Lyse", "Coralie", "Emeline",
]

# ─── FRENCH LAST NAMES (80+) ───
FRENCH_LAST = [
    "Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand", "Leroy", "Moreau",
    "Simon", "Laurent", "Lefebvre", "Michel", "Garcia", "David", "Bertrand", "Roux", "Vincent", "Fournier",
    "Morel", "Girard", "Andre", "Mercier", "Dupont", "Lambert", "Bonnet", "Francois", "Martinez", "Legrand",
    "Garnier", "Faure", "Rousseau", "Blanc", "Muller", "Henry", "Roussel", "Nicolas", "Perrin", "Morin",
    "Mathieu", "Clement", "Gauthier", "Dumont", "Lopez", "Fontaine", "Chevalier", "Robin", "Masson", "Sanchez",
    "Blanchard", "Dumas", "Lemoine", "Picard", "Renault", "Carpentier", "Giraud", "Marchand", "Vidal", "Brun",
    "Leclerc", "Barbier", "Caron", "Delorme", "Etienne", "Gaillard", "Herve", "Joubert", "Lecomte", "Mallet",
    "Neveu", "Perrot", "Rey", "Tanguy", "Vasseur", "Aubert", "Breton", "Collet", "Ferrand", "Germain",
]

# ─── GERMAN MALE FIRST NAMES (60+) ───
GERMAN_MALE_FIRST = [
    "Ben", "Paul", "Finn", "Leon", "Elias", "Jonas", "Noah", "Felix", "Luis", "Luca",
    "Maximilian", "Alexander", "Moritz", "Julian", "Sebastian", "Niklas", "Tobias", "Florian", "Lukas", "Jan",
    "Friedrich", "Heinrich", "Karl", "Ludwig", "Otto", "Wolfgang", "Dieter", "Hans", "Klaus", "Manfred",
    "Stefan", "Andreas", "Christoph", "Dominik", "Erik", "Fabian", "Gregor", "Jens", "Matthias", "Ralf",
    "Thomas", "Werner", "Bernhard", "Detlef", "Gerhard", "Helmut", "Konrad", "Norbert", "Rainer", "Siegfried",
    "Viktor", "Armin", "Bastian", "Clemens", "Dirk", "Gunter", "Holger", "Ingo", "Joachim", "Leonhard",
]

# ─── GERMAN FEMALE FIRST NAMES (60+) ───
GERMAN_FEMALE_FIRST = [
    "Emma", "Mia", "Hannah", "Sophia", "Emilia", "Lina", "Marie", "Mila", "Ella", "Clara",
    "Anna", "Lea", "Lena", "Johanna", "Luisa", "Charlotte", "Maja", "Sophie", "Amelie", "Nele",
    "Katharina", "Elisabeth", "Frieda", "Greta", "Helga", "Ingrid", "Liesel", "Marlene", "Petra", "Ursula",
    "Annika", "Birgit", "Claudia", "Dagmar", "Eva", "Franziska", "Gisela", "Heidi", "Ilse", "Julia",
    "Karin", "Laura", "Monika", "Nina", "Renate", "Sabine", "Stefanie", "Tanja", "Verena", "Andrea",
    "Bettina", "Cornelia", "Dorothea", "Erika", "Gabriele", "Hildegard", "Irmgard", "Jutta", "Katrin", "Lisa",
]

# ─── GERMAN LAST NAMES (60+) ───
GERMAN_LAST = [
    "Muller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker", "Schulz", "Hoffmann",
    "Schafer", "Koch", "Bauer", "Richter", "Klein", "Wolf", "Schroder", "Neumann", "Schwarz", "Zimmermann",
    "Braun", "Kruger", "Hofmann", "Hartmann", "Lange", "Schmitt", "Werner", "Schmitz", "Krause", "Meier",
    "Lehmann", "Schmid", "Schulze", "Maier", "Kohler", "Herrmann", "Walter", "Konig", "Mayer", "Huber",
    "Kaiser", "Fuchs", "Peters", "Lang", "Scholz", "Moller", "Weidner", "Otto", "Stein", "Gross",
    "Roth", "Beck", "Lorenz", "Frank", "Ludwig", "Berger", "Albrecht", "Brandt", "Seidel", "Vogel",
]

# ─── SPANISH MALE FIRST NAMES (60+) ───
SPANISH_MALE_FIRST = [
    "Hugo", "Mateo", "Martin", "Lucas", "Leo", "Daniel", "Alejandro", "Pablo", "Manuel", "Alvaro",
    "Adrian", "David", "Mario", "Diego", "Javier", "Carlos", "Miguel", "Sergio", "Ivan", "Raul",
    "Antonio", "Fernando", "Francisco", "Jorge", "Jose", "Juan", "Luis", "Pedro", "Rafael", "Ramon",
    "Alberto", "Andres", "Eduardo", "Enrique", "Gabriel", "Gonzalo", "Hector", "Ignacio", "Joaquin", "Lorenzo",
    "Marcos", "Nicolas", "Oscar", "Ricardo", "Roberto", "Salvador", "Santiago", "Tomas", "Vicente", "Xavier",
    "Anibal", "Bruno", "Cristian", "Emilio", "Federico", "Gilberto", "Ismael", "Julian", "Mauricio", "Rodrigo",
]

# ─── SPANISH FEMALE FIRST NAMES (60+) ───
SPANISH_FEMALE_FIRST = [
    "Lucia", "Sofia", "Maria", "Martina", "Paula", "Julia", "Daniela", "Valeria", "Alba", "Emma",
    "Carla", "Sara", "Noa", "Carmen", "Claudia", "Valentina", "Adriana", "Alejandra", "Ana", "Elena",
    "Isabel", "Laura", "Marta", "Natalia", "Pilar", "Rosa", "Silvia", "Teresa", "Victoria", "Ximena",
    "Alicia", "Beatriz", "Cristina", "Diana", "Eva", "Gloria", "Irene", "Lola", "Mercedes", "Nuria",
    "Olivia", "Patricia", "Raquel", "Sandra", "Susana", "Yolanda", "Blanca", "Consuelo", "Dolores", "Esperanza",
    "Guadalupe", "Ines", "Josefina", "Leonor", "Marisol", "Paloma", "Rocio", "Soledad", "Veronica", "Amelia",
]

# ─── SPANISH LAST NAMES (60+) ───
SPANISH_LAST = [
    "Garcia", "Rodriguez", "Martinez", "Lopez", "Gonzalez", "Hernandez", "Perez", "Sanchez", "Ramirez", "Torres",
    "Flores", "Rivera", "Gomez", "Diaz", "Reyes", "Cruz", "Morales", "Ortiz", "Gutierrez", "Chavez",
    "Ramos", "Vargas", "Castillo", "Jimenez", "Moreno", "Romero", "Herrera", "Medina", "Aguilar", "Vega",
    "Castro", "Mendez", "Ruiz", "Alvarez", "Fernandez", "Munoz", "Delgado", "Rojas", "Navarro", "Santos",
    "Guerrero", "Cardenas", "Contreras", "Fuentes", "Leon", "Soto", "Suarez", "Campos", "Dominguez", "Espinoza",
    "Acosta", "Bautista", "Cabrera", "Duarte", "Estrada", "Figueroa", "Gallegos", "Ibarra", "Lara", "Molina",
]

# ─── ITALIAN MALE FIRST NAMES (60+) ───
ITALIAN_MALE_FIRST = [
    "Leonardo", "Francesco", "Alessandro", "Lorenzo", "Mattia", "Andrea", "Gabriele", "Riccardo", "Tommaso", "Edoardo",
    "Giuseppe", "Giovanni", "Marco", "Luca", "Stefano", "Roberto", "Antonio", "Massimo", "Vincenzo", "Paolo",
    "Davide", "Federico", "Giacomo", "Nicolo", "Filippo", "Diego", "Emanuele", "Simone", "Matteo", "Daniele",
    "Alberto", "Bruno", "Carlo", "Dario", "Enrico", "Fabio", "Gianluca", "Pietro", "Salvatore", "Sergio",
    "Angelo", "Claudio", "Domenico", "Ezio", "Franco", "Giorgio", "Marcello", "Raffaele", "Silvio", "Vittorio",
    "Aldo", "Cesare", "Dante", "Emilio", "Flavio", "Guido", "Leone", "Mario", "Nino", "Rocco",
]

# ─── ITALIAN FEMALE FIRST NAMES (60+) ───
ITALIAN_FEMALE_FIRST = [
    "Sofia", "Giulia", "Aurora", "Alice", "Ginevra", "Emma", "Giorgia", "Greta", "Beatrice", "Anna",
    "Chiara", "Sara", "Francesca", "Elena", "Valentina", "Alessia", "Martina", "Elisa", "Arianna", "Bianca",
    "Camilla", "Carlotta", "Diana", "Eleonora", "Federica", "Ilaria", "Laura", "Lucia", "Marta", "Paola",
    "Roberta", "Silvia", "Teresa", "Viola", "Clara", "Rosa", "Serena", "Simona", "Viviana", "Angela",
    "Caterina", "Daniela", "Eva", "Flavia", "Gabriella", "Isabella", "Lisa", "Monica", "Nicoletta", "Patrizia",
    "Raffaella", "Stefania", "Veronica", "Alessandra", "Benedetta", "Cecilia", "Delia", "Emanuela", "Giovanna", "Margherita",
]

# ─── ITALIAN LAST NAMES (60+) ───
ITALIAN_LAST = [
    "Rossi", "Russo", "Ferrari", "Esposito", "Bianchi", "Romano", "Colombo", "Ricci", "Marino", "Greco",
    "Bruno", "Gallo", "Conti", "DeLuca", "Mancini", "Costa", "Giordano", "Rizzo", "Lombardi", "Moretti",
    "Barbieri", "Fontana", "Santoro", "Mariani", "Rinaldi", "Caruso", "Ferrara", "Gatti", "Villa", "Leone",
    "Longo", "Martinelli", "Marchetti", "Valentini", "Sala", "Farina", "Pellegrini", "Caputo", "Palumbo", "Serra",
    "Amato", "Basile", "Cattaneo", "DeAngelis", "Ferri", "Grassi", "Marchetti", "Orlando", "Pagano", "Silvestri",
    "Bernardi", "Coppola", "Donati", "Fabbri", "Gentile", "Innocenti", "Montanari", "Parisi", "Riva", "Sorrentino",
]

# ─── AUSTRALIAN MALE FIRST NAMES (60+) ───
AUSTRALIAN_MALE_FIRST = [
    "Jack", "Oliver", "William", "Noah", "James", "Thomas", "Henry", "Charlie", "Leo", "Lucas",
    "Liam", "Ethan", "Mason", "Alexander", "Ryan", "Cooper", "Archer", "Harrison", "Hunter", "Lachlan",
    "Darcy", "Hamish", "Angus", "Callum", "Fletcher", "Jasper", "Ryder", "Finn", "Beau", "Heath",
    "Logan", "Blake", "Ashton", "Riley", "Zac", "Mitchell", "Dylan", "Kai", "Declan", "Nate",
    "Cameron", "Bryce", "Hayden", "Jayden", "Owen", "Tyler", "Brandon", "Caleb", "Dominic", "Gavin",
    "Marcus", "Patrick", "Scott", "Travis", "Wesley", "Brett", "Cody", "Dalton", "Shane", "Trent",
]

# ─── AUSTRALIAN FEMALE FIRST NAMES (60+) ───
AUSTRALIAN_FEMALE_FIRST = [
    "Charlotte", "Olivia", "Amelia", "Isla", "Ava", "Mia", "Grace", "Willow", "Harper", "Chloe",
    "Ella", "Sophie", "Lily", "Zoe", "Emily", "Ruby", "Ivy", "Sienna", "Matilda", "Evelyn",
    "Piper", "Scarlett", "Layla", "Audrey", "Georgia", "Mackenzie", "Tessa", "Tahlia", "Indiana", "Imogen",
    "Kiara", "Bonnie", "Frankie", "Summer", "Billie", "Wren", "Maeve", "Heidi", "Skye", "Jade",
    "Holly", "Jasmine", "Amber", "Brooke", "Paige", "Taylor", "Courtney", "Dana", "Hayley", "Jessica",
    "Megan", "Natalie", "Rachel", "Samantha", "Victoria", "Alexandra", "Brianna", "Chelsea", "Danielle", "Elise",
]

# ─── AUSTRALIAN LAST NAMES (60+) ───
AUSTRALIAN_LAST = [
    "Smith", "Jones", "Williams", "Brown", "Wilson", "Taylor", "Johnson", "White", "Martin", "Anderson",
    "Thompson", "Nguyen", "Thomas", "Walker", "Harris", "Lee", "Ryan", "Robinson", "Kelly", "King",
    "Davis", "Wright", "Evans", "Roberts", "Green", "Hall", "Wood", "Jackson", "Clarke", "Mitchell",
    "Campbell", "Murray", "Bell", "Scott", "Cooper", "Ward", "Turner", "Morgan", "Murphy", "Palmer",
    "Harrison", "Henderson", "Coleman", "Simpson", "Graham", "Hamilton", "Ross", "Fraser", "Stewart", "Gordon",
    "Paterson", "Grant", "Marshall", "Sullivan", "McDonald", "Fitzgerald", "Kennedy", "OBrien", "Doyle", "Walsh",
]


# ==================== GENERATION FUNCTIONS ====================

def _pick_gender():
    """70% male, 30% female."""
    return "M" if random.random() < 0.70 else "F"


# Origin pools mapping — each origin maps to its (male_first, female_first, last) name lists
_ORIGIN_POOLS = {
    "indian": (INDIAN_MALE_FIRST, INDIAN_FEMALE_FIRST, INDIAN_LAST),
    "us":     (US_MALE_FIRST, US_FEMALE_FIRST, US_LAST),
    "uk":     (UK_MALE_FIRST, UK_FEMALE_FIRST, UK_LAST),
    "french":  (FRENCH_MALE_FIRST, FRENCH_FEMALE_FIRST, FRENCH_LAST),
    "german":  (GERMAN_MALE_FIRST, GERMAN_FEMALE_FIRST, GERMAN_LAST),
    "spanish": (SPANISH_MALE_FIRST, SPANISH_FEMALE_FIRST, SPANISH_LAST),
    "italian": (ITALIAN_MALE_FIRST, ITALIAN_FEMALE_FIRST, ITALIAN_LAST),
    "australian": (AUSTRALIAN_MALE_FIRST, AUSTRALIAN_FEMALE_FIRST, AUSTRALIAN_LAST),
}

# Weighted distribution: 10% Indian, 90% international (split across 7 countries)
_ORIGIN_CHOICES = ["indian", "us", "uk", "french", "german", "spanish", "italian", "australian"]
_ORIGIN_WEIGHTS = [10, 18, 15, 13, 12, 12, 10, 10]  # sums to 100


def _pick_origin():
    """10% Indian, 90% international (US/UK/French/German/Spanish/Italian/Australian)."""
    return random.choices(_ORIGIN_CHOICES, weights=_ORIGIN_WEIGHTS, k=1)[0]


def _pick_name(gender, origin):
    """Pick first+last name from same origin pool. Never mix origins."""
    male_first, female_first, last_names = _ORIGIN_POOLS[origin]
    first = random.choice(male_first if gender == "M" else female_first)
    last = random.choice(last_names)
    return first, last


def _generate_dob(min_age=21, max_age=40):
    """Generate a random DOB between min_age and max_age years ago.
    Returns (dob_string, birth_year)."""
    today = datetime.now()
    age = random.randint(min_age, max_age)
    birth_year = today.year - age
    birth_month = random.randint(1, 12)
    birth_day = random.randint(1, 28)  # safe for all months

    dob_date = datetime(birth_year, birth_month, birth_day)
    dob_str = dob_date.strftime("%B %d, %Y")
    return dob_str, birth_year


def _random_code(length=3):
    """Generate a random alphanumeric code like 'k8m', 'x3r', 'p7q'."""
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choices(chars, k=length))


def _generate_email_username(first_name: str, last_name: str, birth_year: int) -> str:
    """
    Generate a BULLETPROOF unique email username using only letters, numbers, and at most one period.
    Gmail only allows letters (a-z), numbers (0-9), and periods (.).
    """
    # Ensure only alphanumeric characters are used to prevent invalid emails
    fn = "".join(c for c in first_name.lower() if c.isalnum())
    ln = "".join(c for c in last_name.lower() if c.isalnum())

    yr = str(birth_year)[-2:]           # "98"
    code2 = _random_code(2)             # "k8"
    code3 = _random_code(3)             # "k8m"
    d2 = str(random.randint(10, 99))    # "47"
    d3 = str(random.randint(100, 999))  # "347"
    code4 = _random_code(4)             # "k8m3"

    patterns = [
        # No dots - Most genuine and common formats
        f"{fn}{ln}{d2}{code2}",             # johnsmith47k8
        f"{fn}{ln}{yr}{code2}",             # johnsmith98k8
        f"{fn}{ln}{code3}",                 # johnsmithk8m
        f"{fn}{ln}{code4}",                 # johnsmithk8m3
        f"{fn[:2]}{ln}{yr}{code3}",         # josmith98k8m
        f"{fn}{d3}{code2}",                 # john347k8
        f"{fn}{ln}{birth_year}{code2}",     # johnsmith1998k8

        # One dot - Still generic and clean
        f"{fn}.{ln}{d2}{code2}",            # john.smith47k8
        f"{fn}.{ln}{yr}{code2}",            # john.smith98k8
        f"{fn}.{ln}{code3}",                # john.smithk8m
        f"{fn}.{ln}{birth_year}{code2}",    # john.smith1998k8
        
        # Other simple variants without special chars
        f"{fn}{code3}{ln}",                 # johnk8msmith
        f"{fn}{d2}{ln}{code2}",             # john47smithk8
        f"{fn[:1]}{ln}{code4}",             # jsmithk8m3
    ]

    weights = [
        15, 15, 12, 10, 8, 5, 8, # No dots
        8, 8, 5, 3,              # One dot
        1, 1, 1,                 # Other variants
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
    70% male / 30% female. 10% Indian / 90% international names. DOB 21-40.
    """
    max_retries = 15
    for _ in range(max_retries):
        gender = _pick_gender()
        origin = _pick_origin()
        first_name, last_name = _pick_name(gender, origin)

        dob_str, birth_year = _generate_dob(21, 40)

        email_user = _generate_email_username(first_name, last_name, birth_year)
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
            # Fetch user_id BEFORE deleting the row
            c.execute("""
                SELECT user_id FROM gmail
                WHERE task_id = %s AND task_status = 'assigned'
            """, (task_id,))
            row = c.fetchone()
            if not row:
                return False

            uid = row['user_id']

            c.execute("""
                DELETE FROM gmail
                WHERE task_id = %s AND task_status = 'assigned'
            """, (task_id,))

            c.execute("""
                UPDATE users SET total_gmail = GREATEST(total_gmail - 1, 0)
                WHERE user_id = %s
            """, (uid,))

            return True
    except Exception as e:
        logger.error(f"Error skipping task {task_id}: {e}")
        return False
