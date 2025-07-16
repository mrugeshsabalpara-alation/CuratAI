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
)
from utils import run_agent


def get_agent(model_provider: str, model_name: str) -> Agent:
    system_prompt = """You are a SQL analyst with tools to help you do this task. Given a user question, use the tools available to answer the user's request. Keep iterating until the user is satisfied with the response."""
    mcp_sql_server = MCPServerStdio(
        "poetry",
        ["run", "npx", "-y", "@executeautomation/database-server", "jira_db.sqlite"],
        timeout=90,
    )
    model = f"{model_provider}:{model_name}"
    agent = Agent(
        model,
        tools=[search_data_products, get_data_product_schema,get_table_info, get_column_info, get_all_fields_for_otype_oid, update_custom_field,propagate_custom_field,get_user_info],
        system_prompt=system_prompt,
        mcp_servers=[mcp_sql_server],
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
        default="openai",
        choices=["openai", "anthropic","bedrock"],
        help="Model provider (openai or anthropic or bedrock)",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-4.1",
        help="Model name (e.g., gpt-4.1, claude-3-5-sonnet-latest)",
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
            result = await run_method(
                agent=agent,
                input_message=user_input,
                message_history=result.new_messages(),
                deps=deps,
            )


if __name__ == "__main__":
    asyncio.run(main())
