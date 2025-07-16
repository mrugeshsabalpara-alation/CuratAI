# Agent demo

This reporsitory provides a library of examples to build AI agent applications. It is built using the pydantic-ai library.

## 1. Setup

If you do not have poetry install, please follow the setup instructions here: https://python-poetry.org/docs/

Then run: 
```
poetry install --no-root
```
Note: since there is no package here (just scripts) you need to add the --no-root flag.

Then add environment variables to your .env file. Make sure one of OPENAI_API_KEY or ANTHROPIC_API_KEY is set and run:
```
source set_env.sh
```

Finally, dowload the jira sqlite database from here and save it in the root of this repository under `jira_db.sqlite`: 
https://drive.google.com/file/d/1hmcKRAIrz6smIxR1ospFTFlJY0VXzXSG/view?usp=sharing

## 2. Run the chat app

Once you ran through the setup steps, you can run the chat app to see agents in action!

```
poetry run python chat.py --provider openai --model gpt-4o 
```
