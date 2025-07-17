"""Simple chat app."""

import argparse
import asyncio
import os
import logfire

from pydantic_ai import Agent
from pydantic_ai.mcp import MCPServerStdio


from auth import AlationAuth
from tools import (
    Dependencies,
    search_data_products,
    get_data_product_schema,
    get_table_info,
    get_column_info,
    get_all_fields_for_otype_oid,
    update_custom_field,
    propagate_custom_field,
    get_user_info,
    get_data_steward_info,
    get_all_folders,
    get_document_info,
    get_all_datasources,
    get_schema_info,
    update_title,
    update_description
)
from utils import run_agent


def get_agent(model_provider: str, model_name: str) -> Agent:
    system_prompt = """
You are CuratAI, an assistant equipped with tools to help users specifically in the data product owner, data steward, data domain—analytics, data engineering, governance, warehousing, etc. Respond only to data-related questions.
Always keep the previous context in mind and use the tools available to answer the user's request.
If the user asks non-data-related questions, respond with humor and do not answer them.
Be concise, clear, and witty—your responses should be short, crisp, and to the point, without losing meaning. Offer alternatives when appropriate.
If user asks about data products, schemas, tables, or columns, provide detailed information. Keys should be in the format "ds_id.schema_name.table_name" (e.g., "1303.ALATION_EDW.RETAIL.HOTEL_GUESTS").
If the user asks about data domains, provide a list of domains and their matching criteria.
Include in responses:
Capabilities of available tools.
A note on consent and safeguards when updating custom fields or modifying data—always flag it.
Keep refining responses until the user is satisfied.


sample user stories with questions are relevant way of returning the response:
Product Owner asks for curation summary
Steward checks missing descriptions and uses "suggest stewards"
All mapped to your Hotel Industries Data Product
:female-office-worker: Final Demo Story: Priya – Data Product Owner
Owns the Hotel Industries Data Product
“Priya is responsible for the overall health and trustworthiness of the Hotel Industries Data Product. It powers reports across Marketing, Ops, and Customer Experience. But she’s not the one editing metadata — she just needs to know: ‘Is everything documented? Who’s responsible?’”
She opens the Data Product in Alation.
 Clicks “CuratAI” and types:
“Give me a curation summary for hotel-related tables.”
:robot_face: CuratAI Responds:
:bed: HOTEL_BOOKINGS
:white_check_mark: All fields documented
:warning: Steward missing
:book: 1 glossary suggestion
:standing_person: HOTEL_GUESTS
:white_check_mark: Fully curated
:closed_lock_with_key: EMAIL flagged as PII, not classified
:credit_card: HOTEL_PAYMENTS
:x: 4 fields missing descriptions
:warning: No steward
:closed_lock_with_key: CC_NUM likely credit card, unclassified
:school_satchel: HOTEL_TRIPTYPE
:white_check_mark: Fully documented
:white_check_mark: Steward assigned: Mrugesh Sabalpara
CuratAI gives her a quick summary:
“Curation: 62% complete
 3 tables missing stewards
 2 fields flagged as PII
 9 fields missing descriptions
 Would you like to → [Send to Stewards] [Auto-curate with AI] [Review Details]”
She clicks “Send to Stewards”, assigning Ravi and Mrugesh automatically. No Slack, no manual work.
“That’s the exact summary I need — without touching a spreadsheet.”
:male-technologist: Final Demo Story: Ravi – Data Steward
Maintains metadata for HOTEL_BOOKINGS & HOTEL_PAYMENTS
“Ravi is the go-to person for fixing table-level metadata. But he's tired of CSV exports, unclear ownership, and long back-and-forths.”
He gets to know from email notification:
“You’ve been suggested as a steward for HOTEL_PAYMENTS. - notificaion through email
He clicks through and asks:
“What columns are missing descriptions?”

Sample 


    """
    mcp_sql_server = MCPServerStdio(
        "poetry",
        ["run", "npx", "-y", "@executeautomation/database-server", "jira_db.sqlite"],
        timeout=90,
    )
    model = f"{model_provider}:{model_name}"
    agent = Agent(
        model,
        tools=[search_data_products,
                get_data_product_schema,
                get_table_info, 
                get_column_info, 
                get_all_fields_for_otype_oid, 
                update_custom_field,
                propagate_custom_field,get_user_info,
                get_all_folders,
                get_document_info,
                get_all_datasources,
                get_schema_info,
                update_title, 
                update_description,
                get_data_steward_info],
        system_prompt=system_prompt,
        # mcp_servers=[mcp_sql_server],
    )
    return agent


async def main():
    # parse command line args to get the model
    parser = argparse.ArgumentParser(
        description="Chat application with model selection"
    )
    parser.add_argument(
        "--provider",
        type=str,
        default="bedrock",
        choices=["openai", "anthropic","bedrock"],
        help="Model provider (openai or anthropic or bedrock)",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
        help="Model name (e.g., gpt-4.1, us.anthropic.claude-3-5-sonnet-20241022-v2:0)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    args = parser.parse_args()

    al_username = os.getenv("ALATION_USERNAME")
    al_password = os.getenv("ALATION_PASSWORD")
    al_base_url = os.getenv("ALATION_BASE_URL")
    al_auth = AlationAuth(al_username, al_password, al_base_url)
    session = al_auth.get_authenticated_session()
    # create the alation session and pass it as an argument to the agent
    agent = get_agent(model_provider=args.provider, model_name=args.model)
    deps = Dependencies(session=session, al_base_url=al_base_url)

    # Add logging
    # Version 1: Local via docker
    # You must have set OTEL_EXPORTER_OTLP_ENDPOINT in your env
    # Run
    # docker run --rm -it -p 4318:4318 --name otel-tui ymtdzzz/otel-tui:latest
    # first before running chat
    logfire.configure(send_to_logfire=False)
    logfire.instrument_pydantic_ai()
    logfire.instrument_httpx(capture_all=True)
    # Version 2: Via Logfire
    # You must have set LOGFIRE_API_TOKEN in your env
    # logfire.configure(token=os.environ.get("LOGFIRE_API_TOKEN"))
    # logfire.instrument_pydantic_ai()

    # use a different run method depending on whether we want to see the log of the agent
    if args.verbose:
        run_method = lambda agent, input_message, message_history, deps: run_agent(
            agent=agent,
            input_message=input_message,
            message_history=message_history,
            run_context=deps,
        )
    else:
        run_method = lambda agent, input_message, message_history, deps: agent.run(
            user_prompt=input_message, message_history=message_history, deps=deps
        )
    def filter_nonempty_messages(messages):
        # Assumes each message is a dict or object with a 'content' field
        #print("Filtering non-empty messages...", messages)
        return [m for m in messages if getattr(m, "content", None) and str(getattr(m, "content")).strip()]

    async with agent.run_mcp_servers():
        result = await run_method(
            agent=agent,
            input_message="Hello. How can you help me?",
            message_history=[],
            deps=deps,
        )
        while True:
            if not args.verbose:
                # only print for non verbose setting, in verbose setting we print the log in run_agent
                print(f"\n{result.output}")
            user_input = input("\n\n> ")
            if user_input == "exit":
                break
            # Filter out empty messages before passing to agent
            filtered_history = result.new_messages()
            result = await run_method(
                agent=agent,
                input_message=user_input,
                message_history=filtered_history,
                deps=deps,
            )


if __name__ == "__main__":
    asyncio.run(main())
