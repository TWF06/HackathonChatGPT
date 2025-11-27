# HACKATHON CHATGPT

Members: \
1.Chong Pohyi\
2.Teo Zheng Xi\
3.Teoh Xi Xian\
4.Tan Wei Feng\
5.Lim Jie Shin


In Peninsular Malaysia, floods happen almost every year. Having a Chat AI to answer questions regarding floods will be definately helpful.\
**📖 Overview**

This project is a Flood Safety Assistant designed to support vulnerable communities in Malaysia during flood emergencies. It uses an embedded Large Language Model (LLM) to generate structured, empathetic responses based on user queries. The system prioritizes accessibility, safety, and clarity — especially for elderly, OKU, infants, and pets.
**Core Capabilities**
**Intelligent Family-First Routing:** Recommends the safest PPS (Public Protection Shelter) based on accessibility and risk.

**Accessibility & Pet Support:** Flags PPS features like wheelchair access, OKU toilets, ramps, and pet policies.

**Structured Output:** Every response includes:

**Recommendation

**Accessibility & Pets

**Rationale (why other PPS options were rejected)


**SafeRoute: Disaster Hub** is here to help to encounter issues and questions regarding floods.

## DATA SET
Due to time limit and avoiding copyright issues for scraping government data, we use a set of mock data for our AI to run.

## JAMAIBASE

**All structured responses are managed in JAMAI under tables like english_prompt and malay_prompt.**

Table Columns:
Column	Description
Name	Scenario or query type (e.g. “public transport”, “OKU access”)
Recommendation	Safest PPS
Accessibility & Pet	Accessibility features and pet policy (e.g. “Mesra kerusi roda, haiwan dibenarkan”)
Rationale	3-point explanation of rejected PPS options
Answer_Queries	Notes on how the assistant should respond to user queries

**Usage:**
Input queries are stored in Name or Answer_Queries.

LLM generates structured outputs for each row.

Backend engineers consume Jama entries as JSON for UI rendering.

All entries are bilingual (English & Bahasa Melayu) and follow strict formatting rules.

## BACKEND
The backend is built using **Python** and **Flask**, designed to run as a serverless function (configured for Vercel). It acts as a middleware between the client-side application and the **JamAI Base** platform, handling data processing and LLM interaction states.

-----

### 🛠️ Tech Stack

  * **Framework:** Flask
  * **AI Orchestration:** [JamAI Base SDK](https://www.jamaibase.com/)
  * **Deployment:** Vercel Serverless Functions

### ⚙️ Environment Variables

To run the backend, the following environment variables must be set (locally in `.env` or in the deployment dashboard):

| Variable | Description |
| :--- | :--- |
| `JAMAI_PROJECT_ID` | Your JamAI Project ID |
| `JAMAI_PAT` | Personal Access Token for JamAI API authentication |
| `ACTION_TABLE_ID` | The ID of the Action Table in JamAI used for processing requests |

### 🔌 API Endpoints

The backend exposes a single unified endpoint that handles the asynchronous nature of LLM processing via a polling mechanism.

#### `POST /api/analyze`

This endpoint operates in two modes: **Job Submission** and **Status Polling**.

**1. Submit Job**
Initiates the analysis by adding a new row to the JamAI Action Table.

  * **Request Body:**
    ```json
    {
      "user_input": "Family of 5, 2 elderly, 1 cat",
      "location_details": "Segamat, Johor"
    }
    ```
  * **Response:** Returns a `row_id` to track the request.
    ```json
    {
      "success": true,
      "status": "submitted",
      "row_id": "row_12345..."
    }
    ```

**2. Fetch Results (Polling)**
Checks if the LLM has finished processing the row.

  * **Request Body:**
    ```json
    {
      "row_id": "row_12345..."
    }
    ```
  * **Response (Pending):**
    ```json
    { "success": false, "status": "pending" }
    ```
  * **Response (Complete):** Returns the structured AI analysis.
    ```json
    {
      "success": true,
      "status": "complete",
      "analysis": "Based on the location...",
      "selected_pps": "Dewan Jubli Intan",
      "tags": "wheelchair_accessible, pet_friendly"
    }
    ```

### 🧠 Logic Flow

1.  **Receive Request:** The app accepts user vulnerabilities and location data.
2.  **JamAI Injection:** Data is pushed to a specific **Action Table** in JamAI. This triggers the embedded LLM columns to generate:
      * `route_analysis` (Reasoning)
      * `selected_pps` (Best shelter match)
      * `decoded_tags` (Standardized vulnerability tags)
3.  **Polling:** The frontend polls the backend using the `row_id`. The backend checks if the specific cells in the table have been populated by the LLM.
4.  **Normalization:** The backend cleans and normalizes the data (e.g., extracting specific shelter names) before returning it to the frontend.
## FRONTEND

## AUTHORS

- [@iztzx](https://github.com/iztzx)
- [@JieShin27](https://github.com/JieShin27)
- [@Pohyi118](https://github.com/Pohyi118)
- [@TeohXiXian-2025](https://github.com/TeohXiXian-2025)
- [@TWF06](https://github.com/TWF06)

## SLIDE AND DEMO

https://www.canva.com/design/DAG5rgvK9O0/sVuH6uLgxUhIjR6qDCZqWQ/view

