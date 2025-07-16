"""Agent utils."""

from pydantic_ai import Agent
from pydantic_ai.messages import (
    FinalResultEvent,
    FunctionToolCallEvent,
    FunctionToolResultEvent,
    ModelMessage,
    PartDeltaEvent,
    PartStartEvent,
    TextPartDelta,
    ThinkingPartDelta,
    ToolCallPartDelta,
)
from rich.console import Console

from tools import Dependencies
from pydantic_ai.agent import AgentRunResult


console = Console()


async def run_agent(
    agent: Agent,
    input_message: str,
    message_history: list[ModelMessage],
    run_context: Dependencies,
) -> AgentRunResult | None:
    final_response: AgentRunResult | None = None
    try:
        # Begin a node-by-node, streaming iteration
        async with agent.iter(
            input_message, message_history=message_history, deps=run_context
        ) as run:
            async for node in run:
                if Agent.is_user_prompt_node(node):
                    # A user prompt node => The user has provided input
                    pass
                elif Agent.is_model_request_node(node):
                    # A model request node => We can stream tokens from the model's request
                    cur_event_id = 0
                    async with node.stream(run.ctx) as request_stream:
                        async for event in request_stream:
                            if isinstance(event, PartStartEvent):
                                pass
                            elif isinstance(event, PartDeltaEvent):
                                if event.index > cur_event_id:
                                    cur_event_id = event.index
                                    console.print()
                                if isinstance(event.delta, TextPartDelta):
                                    console.print(
                                        event.delta.content_delta,
                                        sep="",
                                        end="",
                                    )
                                elif isinstance(event.delta, ThinkingPartDelta):
                                    console.print(
                                        event.delta.content_delta,
                                        sep="",
                                        end="",
                                    )
                                elif isinstance(event.delta, ToolCallPartDelta):
                                    pass
                            elif isinstance(event, FinalResultEvent):
                                pass
                elif Agent.is_call_tools_node(node):
                    # A handle-response node => The model returned some data, potentially calls a tool
                    async with node.stream(run.ctx) as handle_stream:
                        async for event in handle_stream:
                            if isinstance(event, FunctionToolCallEvent):
                                console.print(
                                    f"[italic magenta]The LLM calls tool={event.part.tool_name!r} with args={event.part.args} (tool_call_id={event.part.tool_call_id!r})\n[/italic magenta]"
                                )
                            elif isinstance(event, FunctionToolResultEvent):
                                pass
                elif Agent.is_end_node(node):
                    assert run.result.output == node.data.output
                    final_response = run.result
                else:
                    raise ValueError(f"Unknown node type: {type(node)}")
    except Exception as e:
        console.print(e)
    return final_response
