# app/streamlit_app.py
import os
import json
import pandas as pd
import streamlit as st
from neo4j import GraphDatabase
from openai import OpenAI
import re
from datetime import datetime

st.set_page_config(page_title="Employee Knowledge Chatbot", page_icon="🧠", layout="wide")

# ---- Environment ----
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER", os.getenv("NEO4J_USERNAME", "neo4j"))
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# ---- Clients ----
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) if NEO4J_URI and NEO4J_PASSWORD else None
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

def run_cypher(query: str, params: dict | None = None):
    if driver is None:
        raise RuntimeError("Neo4j driver is not configured. Set NEO4J_URI/NEO4J_PASSWORD.")
    with driver.session(database=NEO4J_DATABASE) as s:
        return s.run(query, **(params or {})).data()

def generate_ai_summary(question: str, data: list, query_type: str) -> str:
    """Generate a conversational AI summary based on REAL data."""
    if not client or not data:
        return generate_accurate_summary(data, question, query_type)
    
    try:
        # Create a factual context from the actual data
        df = pd.DataFrame(data)
        
        # Build factual context string
        context_parts = [f"User asked: '{question}'"]
        context_parts.append(f"Found {len(data)} record(s) in the database:")
        
        # Add specific facts based on query type
        if query_type == "employee_search" and len(data) > 0:
            employee = data[0]
            context_parts.append(f"Employee: {employee.get('name', 'N/A')}")
            context_parts.append(f"Role: {employee.get('designation', 'N/A')}")
            context_parts.append(f"Employee ID: {employee.get('employee_id', 'N/A')}")
            if 'skills' in employee:
                context_parts.append(f"Skills: {', '.join(employee['skills'])}")
            if 'projects' in employee:
                context_parts.append(f"Projects: {', '.join(employee['projects'])}")
            if 'manager' in employee:
                context_parts.append(f"Manager: {employee['manager']}")
                
        elif query_type == "skills" and len(data) > 0:
            skills_summary = df.groupby('skill')['employee'].count().to_dict()
            context_parts.append(f"Skills distribution: {skills_summary}")
            context_parts.append(f"Sample employees with these skills: {', '.join(df['employee'].head(5).tolist())}")
            
        elif query_type == "projects" and len(data) > 0:
            projects_summary = df.groupby('project')['employee'].count().to_dict()
            context_parts.append(f"Projects distribution: {projects_summary}")
            
        elif query_type == "employees" and len(data) > 0:
            roles_summary = df['designation'].value_counts().head(5).to_dict()
            context_parts.append(f"Top roles: {roles_summary}")
            context_parts.append(f"Total employees found: {len(data)}")
        
        context = "\n".join(context_parts)
        
        prompt = f"""
        Based on the EXACT database results below, provide a helpful, accurate summary. 
        DO NOT invent or hallucinate information. Only use what's in the data.
        
        {context}
        
        Provide:
        1. A direct answer to the user's question using ONLY the data above
        2. Key factual observations from the data
        3. If no data found, clearly state that
        
        Be conversational but strictly factual.
        """
        
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You are a precise HR assistant that only uses provided data. Never invent information."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=300
        )
        
        return response.choices[0].message.content.strip()
        
    except Exception as e:
        return generate_accurate_summary(data, question, query_type)

def generate_accurate_summary(data: list, question: str, query_type: str) -> str:
    """Generate a 100% accurate summary without AI."""
    if not data:
        return "❌ No matching records found in the database for your query."
    
    df = pd.DataFrame(data)
    
    summary_parts = [f"✅ Found {len(data)} matching record(s) in the database:"]
    
    if query_type == "employee_search" and len(data) > 0:
        employee = data[0]
        summary_parts.append(f"**Employee:** {employee.get('name', 'N/A')}")
        summary_parts.append(f"**Role:** {employee.get('designation', 'N/A')}")
        summary_parts.append(f"**Employee ID:** {employee.get('employee_id', 'N/A')}")
        summary_parts.append(f"**Date Joined:** {employee.get('date_of_joining', 'N/A')}")
        
        if 'skills' in employee and employee['skills']:
            summary_parts.append(f"**Skills:** {', '.join(employee['skills'])}")
        if 'projects' in employee and employee['projects']:
            summary_parts.append(f"**Projects:** {', '.join(employee['projects'])}")
        if 'manager' in employee:
            summary_parts.append(f"**Manager:** {employee['manager']}")
            
    elif query_type == "skills":
        unique_skills = df['skill'].unique()
        summary_parts.append(f"**Skills found:** {', '.join(unique_skills)}")
        summary_parts.append(f"**Employees with these skills:** {len(df['employee'].unique())}")
        
    elif query_type == "projects":
        unique_projects = df['project'].unique()
        summary_parts.append(f"**Projects found:** {', '.join(unique_projects)}")
        summary_parts.append(f"**Employees on these projects:** {len(df['employee'].unique())}")
        
    elif query_type == "employees":
        unique_roles = df['designation'].value_counts().head(5)
        roles_text = ', '.join([f"{role} ({count})" for role, count in unique_roles.items()])
        summary_parts.append(f"**Role distribution:** {roles_text}")
    
    return "\n\n".join(summary_parts)

def get_comprehensive_employee_data(name: str) -> dict:
    """Get ALL available data for a specific employee from Neo4j."""
    # Main employee query
    employee_query = """
    MATCH (e:Employee) 
    WHERE toLower(e.name) CONTAINS toLower($name)
    RETURN e.empId AS employee_id, e.name AS name, e.designation AS designation, 
           e.doj AS date_of_joining, e.gender AS gender
    LIMIT 1
    """
    
    # Skills query
    skills_query = """
    MATCH (e:Employee)-[hs:HAS_SKILL]->(s:Skill)
    WHERE toLower(e.name) CONTAINS toLower($name)
    RETURN collect(DISTINCT s.name) AS skills
    """
    
    # Projects query
    projects_query = """
    MATCH (e:Employee)-[w:WORKS_ON]->(p:Project)
    WHERE toLower(e.name) CONTAINS toLower($name)
    RETURN collect(DISTINCT p.name) AS projects
    """
    
    # Manager query
    manager_query = """
    MATCH (e:Employee)-[:REPORTS_TO]->(m:Employee)
    WHERE toLower(e.name) CONTAINS toLower($name)
    RETURN m.name AS manager
    LIMIT 1
    """
    
    try:
        # Execute all queries
        basic_info = run_cypher(employee_query, {"name": name})
        if not basic_info:
            return None
            
        employee_data = basic_info[0]
        
        # Get skills
        skills_result = run_cypher(skills_query, {"name": name})
        if skills_result and skills_result[0]['skills']:
            employee_data['skills'] = skills_result[0]['skills']
        
        # Get projects
        projects_result = run_cypher(projects_query, {"name": name})
        if projects_result and projects_result[0]['projects']:
            employee_data['projects'] = projects_result[0]['projects']
        
        # Get manager
        manager_result = run_cypher(manager_query, {"name": name})
        if manager_result:
            employee_data['manager'] = manager_result[0]['manager']
        
        return employee_data
        
    except Exception as e:
        st.error(f"Database query error: {e}")
        return None

# ---- UI ----
st.title("🧠 Employee Knowledge Chatbot")
st.caption("Ask me anything about employees, skills, projects, or organizational structure!")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if "data" in message and message["data"] is not None:
            st.dataframe(pd.DataFrame(message["data"]), width='stretch')
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask about employees, skills, projects..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display user message
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Generate response
    with st.chat_message("assistant"):
        with st.spinner("Searching database..."):
            prompt_lower = prompt.lower()
            data = None
            query_type = "general"
            
            # Employee-specific search
            if any(word in prompt_lower for word in ['malavika', 'omkar', 'john', 'alice', 'about', 'tell me about', 'details of']):
                name_match = re.search(r'(?:about|tell me about|show|find|details of)\s+([a-zA-Z\s]+)', prompt_lower)
                employee_name = name_match.group(1).strip() if name_match else prompt
                
                employee_data = get_comprehensive_employee_data(employee_name)
                
                if employee_data:
                    data = [employee_data]
                    query_type = "employee_search"
                    response_text = generate_ai_summary(prompt, data, query_type)
                else:
                    response_text = f"❌ No employee found with name containing '{employee_name}' in the database."
                    data = None
                    
            # Skills search
            elif any(word in prompt_lower for word in ['python', 'java', 'skill', 'react', 'knows', 'knowledge']):
                skill_match = re.search(r'(?:knows?|with|has|skill[s]?)\s+([a-zA-Z\s]+)', prompt_lower)
                skill = skill_match.group(1).strip() if skill_match else "python"
                
                cypher = """
                MATCH (e:Employee)-[:HAS_SKILL]->(s:Skill)
                WHERE toLower(s.name) CONTAINS toLower($skill)
                RETURN e.name AS employee, e.designation AS designation, 
                       e.empId AS employee_id, s.name AS skill, e.doj AS date_of_joining
                ORDER BY e.name
                """
                data = run_cypher(cypher, {"skill": skill})
                query_type = "skills"
                response_text = generate_ai_summary(prompt, data, query_type)
                
            # Projects search
            elif any(word in prompt_lower for word in ['project', 'working on', 'team']):
                cypher = """
                MATCH (e:Employee)-[:WORKS_ON]->(p:Project)
                RETURN e.name AS employee, e.designation AS designation,
                       p.name AS project, e.empId AS employee_id, e.doj AS date_of_joining
                ORDER BY p.name, e.name
                LIMIT 50
                """
                data = run_cypher(cypher)
                query_type = "projects"
                response_text = generate_ai_summary(prompt, data, query_type)
                
            # All employees
            elif any(word in prompt_lower for word in ['all', 'list', 'show', 'employees']):
                cypher = """
                MATCH (e:Employee)
                RETURN e.name AS employee, e.designation AS designation,
                       e.empId AS employee_id, e.doj AS date_of_joining
                ORDER BY e.name
                LIMIT 100
                """
                data = run_cypher(cypher)
                query_type = "employees"
                response_text = generate_ai_summary(prompt, data, query_type)
                
            else:
                # General search
                cypher = """
                MATCH (e:Employee)
                WHERE toLower(e.name) CONTAINS toLower($search) OR 
                      toLower(e.designation) CONTAINS toLower($search)
                RETURN e.name AS employee, e.designation AS designation,
                       e.empId AS employee_id, e.doj AS date_of_joining
                LIMIT 50
                """
                data = run_cypher(cypher, {"search": prompt})
                query_type = "general"
                response_text = generate_ai_summary(prompt, data, query_type)
            
            # Display results
            if data and len(data) > 0:
                # Clean data for display
                display_data = []
                for item in data:
                    clean_item = {}
                    for key, value in item.items():
                        if isinstance(value, list):
                            clean_item[key] = ", ".join(value) if value else "None"
                        else:
                            clean_item[key] = value if value is not None else "Not specified"
                    display_data.append(clean_item)
                
                st.dataframe(pd.DataFrame(display_data), width='stretch')
            
            st.markdown(response_text)
            
            # Add to chat history
            st.session_state.messages.append({
                "role": "assistant", 
                "content": response_text,
                "data": data
            })

# Sidebar with database info
with st.sidebar:
    st.header("📊 Database Info")
    
    if st.button("Check Database Connection"):
        try:
            test_query = "MATCH (e:Employee) RETURN count(e) AS total_employees"
            result = run_cypher(test_query)
            st.success(f"✅ Connected! Total employees: {result[0]['total_employees']}")
            
            # Show some stats
            skills_count = run_cypher("MATCH (s:Skill) RETURN count(s) AS skills")[0]['skills']
            projects_count = run_cypher("MATCH (p:Project) RETURN count(p) AS projects")[0]['projects']
            st.info(f"Skills: {skills_count} | Projects: {projects_count}")
            
        except Exception as e:
            st.error(f"❌ Connection failed: {e}")
    
    st.header("💡 Example Questions")
    st.markdown("""
    **Find Employees:**
    - `Malavika Patra`
    - `Tell me about Omkar Khandagale`
    - `Who is John Doe?`
    
    **Skills Search:**
    - `Who knows Python?`
    - `Employees with Java skills`
    - `Show React developers`
    
    **Projects & Teams:**
    - `Show all projects`
    - `Who works on Project X?`
    
    **General:**
    - `Show all employees`
    - `List software engineers`
    - `Recent hires`
    """)
    
    if st.button("Clear Chat"):
        st.session_state.messages = []
        st.rerun()