"""Agent tools."""

from dataclasses import dataclass

import requests
from pydantic_ai import ModelRetry, RunContext
import time

@dataclass
class Dependencies:
    session: requests.Session
    al_base_url: str


def get_data_product_schema(ctx: RunContext[Dependencies], product_id: str) -> str:
    """Get model-friendly schema for model context.

    Args:
        product_id: ID of the data product
    """
    session = ctx.deps.session
    api_url = f"/integration/data-products/v1/data-product/{product_id}/"
    response = session.get(ctx.deps.al_base_url + api_url)
    response.raise_for_status()
    res = response.json()
    schema_str = ""
    content = res["spec_json"]["product"]["recordSets"]
    for table in content:
        schema_str += f"table: {table}\n"
        for col in content[table]["schema"]:
            schema_str += f"\tcolumn: {col['name']}\n\ttype: {col['type']}\n\tdescription: {col['description']}\n\n"
    return schema_str

def search_data_products(
    ctx: RunContext[Dependencies],
    search_term: str,
    limit: int = 100,
) -> str:
    """Search data products that match a specific search term.

    Will return a list of data products with their ID, name, description, and owner. It will also list the total number of data products.

    Args:
        search_term: Search term to filter data products by name
        limit: The maximum number of results to return. Limit can be no more than 100
    """
    if limit > 100:
        raise ModelRetry("Limit can be no more than 100.")
    session = ctx.deps.session
    api_url = "/integration/data-products/v1/data-product/"
    response = session.get(ctx.deps.al_base_url + api_url)
    response.raise_for_status()
    # Returns a list of DP specs
    res = response.json()
    products = []
    total = len(res)
    for product in res:
        product_metadata = product["spec_json"]["product"]["en"]
        if search_term and search_term.lower() not in product_metadata["name"].lower():
            continue
        product_dict = {
            "id": product["spec_json"]["product"]["productId"],
            "name": product_metadata["name"].strip(),
            "description": product_metadata["description"][:100].strip()
            + (" ..." if len(product_metadata["description"]) > 100 else ""),
            "owner": product["spec_json"]["product"]["contactName"].strip(),
        }
        products.append(
            f"- id: {product_dict['id']}\n"
            f"  name: {product_dict['name']}\n"
            f"  description: {product_dict['description']}\n"
            f"  owner: {product_dict['owner']}\n"
        )
    serialized_products = "\n".join(products[:limit])
    return f"Total Products Available: {total}\nLimit: {limit}\n--------\n\n{serialized_products}"

def get_table_info(ctx: RunContext[Dependencies], table_name: str = None, key: str = None) -> str:
    """Get information about a specific table present in the data product.

    Args:
        table_name: Name of the table to get information about
        key: Unique key of the table (optional)
    """
    session = ctx.deps.session

    if key:
        # Fetch by key (assume key is unique)
        api_url = f"/integration/v2/table/?id={key}"
    elif table_name:
        api_url = f"/integration/v2/table/?name__iexact={table_name}&limit=100&skip=0"
    else:
        return "Please provide either 'table_name' or 'key'."

    response = session.get(ctx.deps.al_base_url + api_url)
    response.raise_for_status()
    tables = response.json()
    if not tables:
        if key:
            return f"No table found with key '{key}'."
        return f"No table found with name '{table_name}'."
    if len(tables) > 1 and not key:
        msg = "Multiple tables found:\n"
        for t in tables:
            fq_name = t.get('fully_qualified_name') or f"{t.get('schema_name', 'N/A')}.{t['name']}"
            msg += (
                f"- id: {t['id']}, name: {t['name']}, fully qualified name: {fq_name}\n"
            )
        msg += "Please specify the fully qualified name or use the 'key' parameter to narrow down your search."
        return msg
    table = tables[0]
    info = (
        f"Table ID: {table['id']}\n"
        f"Table Key: {table.get('key', '')}\n"
        f"Table Name: {table['name']}\n"
        f"Schema: {table.get('schema_name', 'N/A')}\n"
        f"Title: {table.get('title', '')}\n"
        f"Description: {table.get('description', '')}\n"
        f"Table Type: {table.get('table_type', '')}\n"
        f"SQL: {table.get('sql', '')}\n"
        f"Comment: {table.get('table_comment', '')}\n"
    )
    if table.get("custom_fields"):
        info += "Custom Fields:\n"
        for field in table["custom_fields"]:
            info += f"  - {field['field_name']}: {field['value']}\n"
    return info


def get_column_info(ctx: RunContext[Dependencies], table_name: str, column_name: str) -> str:
    """Get information about a specific column in a table.

    Args:
        table_name: Name of the table
        column_name: Name of the column to get information about
    """
    session = ctx.deps.session
    api_url = f"/integration/v2/column/?name__iexact={column_name}&limit=100&skip=0"
    response = session.get(ctx.deps.al_base_url + api_url)
    response.raise_for_status()
    columns = response.json()
    if not columns:
        return f"No column found with name '{column_name}'."
    if len(columns) > 1:
        msg = "Multiple columns found:\n"
        for t in columns:
            # Show fully qualified name if possible
            fq_name = t.get('fully_qualified_name') or f"{t.get('schema_name', 'N/A')}.{t.get('table_name', 'N/A')}.{t['name']}"
            msg += (
                f"- id: {t['id']}, name: {t['name']}, fully qualified name: {fq_name}\n"
            )
        msg += "Please specify the fully qualified name to narrow down your search."
        return msg
    col = columns[0]

    if col["name"] == column_name:
        info = (
            f"Column ID: {col.get('id', 'N/A')}\n"
            f"Column Key: {col.get('key', '')}\n"
            f"Column Name: {col['name']}\n"
            f"Type: {col.get('type', '')}\n"
            f"Nullable: {col.get('nullable', '')}\n"
            f"Default: {col.get('default', '')}\n"
            f"Description: {col.get('description', '')}\n"
        )
        if col.get("custom_fields"):
            info += "Custom Fields:\n"
            for field in col["custom_fields"]:
                info += f"  - {field['field_name']}: {field['value']}\n"
        return info
    return f"No column named '{column_name}' found in table '{table_name}'."


def get_all_fields_for_otype_oid(ctx: RunContext[Dependencies], otype: str, oid: str) -> str:
    """
    Get all custom fields that can be updated for a given object type and id. If user asks for custom fields, then first run the get_table_info or get_column_info function if it is table or column respectively.

    Args:
        otype: The object type (for example, 'table' or 'column')
        oid: The object id

    Returns:
        List of custom fields that can be updated for the specified object.
    """
    if not otype or not oid:
        return "Both 'otype' (object type) and 'oid' (object id) must be provided."
    session = ctx.deps.session
    api_url = f"/api/field/object/{otype}/{oid}/"
    response = session.get(ctx.deps.al_base_url + api_url)
    response.raise_for_status()
    fields = response.json()
    if not fields:
        return f"No custom fields found for {otype} with id '{oid}'."
    info = f"Custom fields for {otype} (id: {oid}):\n"
    from pprint import pprint as pp
    print("object type:", otype, "object id:", oid)
    all_fields = fields.get("all_fields", {})
    if not all_fields:
        return f"No custom fields found for {otype} with id '{oid}'."
    for field_id, field in all_fields.items():
        info += (
            f"- Field ID: {field.get('field_id', '')}\n"
            f"  Field Name: {field.get('name', '')}\n"
            f"  Type: {field.get('type', '')}\n"
            f"  Description: {field.get('description', '')}\n"
            f"  Editable: {field.get('is_editable', False)}\n"
            f"  Value: {field.get('value', '')}\n"
        )
    return info

def update_custom_field(
    ctx: RunContext[Dependencies],
    otype: str,
    object_id: str,
    field_id: int,
    value,
    operation: str,
) -> str:
    """
    Update a custom field for a given object.

    Args:
        ctx: RunContext with dependencies
        otype: The object type (e.g., 'table', 'column')
        object_id: The object ID
        field_id: The custom field ID
        value: The value to set/add/remove (can be string, int, or list depending on field type)
        operation: One of 'replace', 'add', or 'remove'
            - 'replace': Overwrite the existing value (for all field types)
            - 'add': Add value(s) to object_set or multipicker fields
            - 'remove': Remove value(s) from object_set or multipicker fields

    Returns:
        Result message indicating success or error.
    """
    session = ctx.deps.session
    url = f"{ctx.deps.al_base_url}/api/field/object/{otype}/{object_id}/{field_id}/commit/"
    headers = {"Content-Type": "application/json"}

    # Prepare payload based on operation and value type
    payload = {"op": operation}

    # For object_set/multipicker, value should be a list; for others, a single value
    # if isinstance(value, list):
    #     payload["value"] = value
    # else:
    #     payload["value"] = value

    print("Value type:", type(value))
    print("Value content:", value)
    if isinstance(value, list):
        value = value[0] if len(value) == 1 else value
    #payload["value"] = { "otype": "user", "oid": value}
    payload["value"] = value

    print("Updating custom field with payload:", payload)
    if operation not in ("replace", "add", "remove"):
        print(f"Invalid operation '{operation}'. Must be one of: replace, add, remove.")
    try:
        response = session.post(url, json=payload, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        return f"Error updating custom field: {e} - {getattr(e.response, 'text', '')}"

    return (
        f"Custom field '{field_id}' updated on {otype} '{object_id}' "
        f"with operation '{operation}'."
    )

def update_title(
    ctx: RunContext[Dependencies],
    otype: str,
    object_name: str,
    key: str,
    value,
) -> str:
    """
    Update a title for a given object.

    Args:
        ctx: RunContext with dependencies
        otype: The object type (e.g., 'table', 'column')
        object_name: The object name
        key: The object key
        value: The value to set/add/remove (can be string, int, or list depending on field type)

    Returns:
        Result message indicating success or error.
    """
    session = ctx.deps.session

    if not key:
        if object_name:
            if otype == 'table':
                table_info = get_table_info(ctx, table_name=object_name)
                key = get_key_from_object_info(table_info)
            if otype == 'attribute':
                column_info = get_column_info(ctx, column_name=object_name)
                key = get_key_from_object_info(column_info)
        else:
            return f"Please provide a {otype} name or key"
    if key:
        url = f"{ctx.deps.al_base_url}/integration/v2/table/"
        payload = {"key": key, "title": value}
        headers = {"Content-Type": "application/json"}
        try:
            response = session.post(url, json=payload, headers=headers)
            response.raise_for_status()
        except requests.RequestException as e:
            return f"Error updating title: {e} - {getattr(e.response, 'text', '')}"

        return (
        f"Title is successfully updated for {otype} '{object_name}' "
        )
    else:
        return "No {otype} found with name {object_name}."

def update_description(
    ctx: RunContext[Dependencies],
    otype: str,
    object_name: str,
    key: str,
    value,
) -> str:
    """
    Update a description for a given object.

    Args:
        ctx: RunContext with dependencies
        otype: The object type (e.g., 'table', 'column')
        object_name: The object name
        key: The object key
        value: The value to set/add/remove (can be string, int, or list depending on field type)

    Returns:
        Result message indicating success or error.
    """
    session = ctx.deps.session

    if not key:
        if object_name:
            if otype == 'table':
                table_info = get_table_info(ctx, table_name=object_name)
                key = get_key_from_object_info(table_info)
            if otype == 'attribute':
                column_info = get_column_info(ctx, column_name=object_name)
                key = get_key_from_object_info(column_info)
        else:
            return f"Please provide a {otype} name or key"
    if key:
        url = f"{ctx.deps.al_base_url}/integration/v2/table/"
        payload = {"key": key, "description": value}
        headers = {"Content-Type": "application/json"}
        try:
            response = session.post(url, json=payload, headers=headers)
            response.raise_for_status()
        except requests.RequestException as e:
            return f"Error updating description: {e} - {getattr(e.response, 'text', '')}"

        return (
            f"Description is successfully updated for {otype} '{object_name}' "
        )
    else:
         return "No {otype} found with name {object_name}."   


def propagate_custom_field(
    ctx: RunContext[Dependencies],
    object_type: str,
    object_id: str,
    field_id: int,
    value,
    operation: str = "replace",
    consent: bool = False,
    direction: str = "downstream",
    target_scope: list = None,
) -> str:
    """
    Propagate a custom field update from an object to its children (downstream) or parents (upstream).

    Args:
        ctx: RunContext with dependencies
        object_type: The object type to propagate from (e.g., 'table', 'column')
        object_id: The object ID to propagate from
        field_id: The custom field ID to update
        value: The value to assign (user name, user ID, or string). Format would be {"otype": "user", "oid": user_id} if user ID is provided.
        operation: One of 'replace', 'add', or 'remove' (default: 'replace')
        consent: Boolean indicating user consent to propagate the update
        direction: 'downstream' (children) or 'upstream' (parents)
        target_scope: List of target asset types (e.g., ['attribute'] for columns, ['table'] for parent tables)

    Returns:
        Status message with job info URL or error.
    """
    if not consent:
        return (
            f"This action will propagate the custom field update from the {object_type} to its "
            f"{'children' if direction == 'downstream' else 'parents'}. "
            "Please confirm your consent by setting 'consent=True'."
        )

    if operation not in ("add", "replace", "remove"):
        return "Invalid operation. Must be one of: add, replace, remove."

    if direction not in ("downstream", "upstream"):
        return "Invalid direction. Must be 'downstream' or 'upstream'."

    if not target_scope:
        # Default: downstream to columns if object_type is table, upstream to tables if object_type is column
        if direction == "downstream" and object_type == "table":
            target_scope = ["attribute"]
        elif direction == "upstream" and object_type == "column":
            target_scope = ["table", "schema", "data"]
        else:
            return "Please specify 'target_scope' for this propagation."

    if isinstance(value, list):
        resolved_value = value
    else:
        resolved_value = [value] if value else []

    payload = {
        "process_virtual_rule": {
            "condition": {
                "op": "assets-from-pivot-hierarchy",
                "operand": object_type,
                "value": object_id,
                "span": {
                    "direction": direction,
                    direction: {
                        "scope": target_scope
                    }
                },
                "extra": {"field_id": field_id},
            },
            "action": {
                "action": "update-field",
                "params": {
                    "field_id": field_id,
                    "value": resolved_value,
                    "op": operation
                }
            }
        }
    }

    url = f"{ctx.deps.al_base_url}/api/curation/assistant/v1/action/"
    headers = {"Content-Type": "application/json"}
    try:
        response = ctx.deps.session.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        return f"Error propagating field: {e} - {getattr(e.response, 'text', '')}"

    try:
        resp_json = response.json()
    except Exception:
        return f"Non-JSON response from server: {response.text}"
    task = resp_json.get("task")
    if not task or not task.get("id"):
        return f"Unexpected response: {resp_json}"
    job_id = task["id"]
    job_url = f"{ctx.deps.al_base_url}/api/job/{job_id}/"

    # Poll for job completion (up to 60 seconds)
    for _ in range(60):
        try:
            job_resp = ctx.deps.session.get(job_url, headers=headers, timeout=5)
            job_resp.raise_for_status()
            job_data = job_resp.json()
        except Exception as e:
            return f"Error checking job status: {e}"
        status = job_data.get("status", "").lower()
        state = job_data.get("state", "").lower()
        if status in ("succeeded", "partial_success") and state == "finished":
            return (
                f"Propagation of field '{field_id}' from {object_type} '{object_id}' to its "
                f"{'children' if direction == 'downstream' else 'parents'} completed successfully.\n"
                f"Job info: {job_url}"
            )
        if status in ("failed", "did_not_start", "skipped"):
            return f"Propagation failed. Job info: {job_url}"
        time.sleep(1)
    return f"Propagation did not complete within timeout. Check job status at: {job_url}"

def get_user_info(ctx: RunContext[Dependencies], user_name: str = None, email: str = None) -> str:

    """
    Get information about a user by name or email.

    Args:
        ctx: RunContext with dependencies
        user_name: The display name of the user (optional)
        email: The email address of the user (optional)

    Returns:
        str: Information about the user or an error message
    """
    if not user_name and not email:
        return "Please provide either 'user_name' or 'email' to fetch user information."

    session = ctx.deps.session
    if email:
        api_url = f"/integration/v2/user/?email={email}"
    else:
        api_url = f"/integration/v2/user/?display_name__icontains={user_name}"

    response = session.get(ctx.deps.al_base_url + api_url)
    response.raise_for_status()
    users = response.json()
    if not users:
        return "No user found with the provided information."
    if len(users) > 1:
        msg = "Multiple users found:\n"
        for user in users:
            msg += (
                f"- id: {user.get('id', 'N/A')}, "
                f"name: {user.get('display_name', 'N/A')}, "
                f"email: {user.get('email', 'N/A')}\n"
            )
        msg += "Please refine your search."
        return msg
    user = users[0]
    info = (
        f"User ID: {user.get('id', 'N/A')}\n"
        f"Display Name: {user.get('display_name', 'N/A')}\n"
        f"Email: {user.get('email', 'N/A')}\n"
        f"Username: {user.get('username', 'N/A')}\n"
        f"Active: {user.get('is_active', 'N/A')}\n"
        f"Role: {user.get('role', 'N/A')}\n"
    )
    return info



############################### check if works ###############
def get_folder_info(ctx: RunContext[Dependencies], folder_id: str) -> str:
    """
    Get information about a specific folder by its ID.

    Args:
        ctx: RunContext with dependencies
        folder_id: The ID of the folder to get information about

    Returns:
        str: Information about the folder or an error message
    """
    session = ctx.deps.session
    api_url = f"/integration/v2/folder/{folder_id}/"
    response = session.get(ctx.deps.al_base_url + api_url)
    response.raise_for_status()
    folder = response.json()
    if not folder:
        return f"No folder found with ID '{folder_id}'."
    
    info = (
        f"Folder ID: {folder.get('id', 'N/A')}\n"
        f"Name: {folder.get('name', 'N/A')}\n"
        f"Description: {folder.get('description', 'N/A')}\n"
        f"Owner: {folder.get('owner', 'N/A')}\n"
        f"Created At: {folder.get('created_at', 'N/A')}\n"
        f"Updated At: {folder.get('updated_at', 'N/A')}\n"
    )
    return info



def get_groupfile_info(ctx: RunContext[Dependencies], groupfile_id: str) -> str:
    """
    Get information about a specific group file by its ID.

    Args:
        ctx: RunContext with dependencies
        groupfile_id: The ID of the group file to get information about

    Returns:
        str: Information about the group file or an error message
    """
    session = ctx.deps.session
    api_url = f"/integration/v2/groupfile/{groupfile_id}/"
    response = session.get(ctx.deps.al_base_url + api_url)
    response.raise_for_status()
    groupfile = response.json()
    if not groupfile:
        return f"No group file found with ID '{groupfile_id}'."
    
    info = (
        f"Group File ID: {groupfile.get('id', 'N/A')}\n"
        f"Name: {groupfile.get('name', 'N/A')}\n"
        f"Description: {groupfile.get('description', 'N/A')}\n"
        f"Owner: {groupfile.get('owner', 'N/A')}\n"
        f"Created At: {groupfile.get('created_at', 'N/A')}\n"
        f"Updated At: {groupfile.get('updated_at', 'N/A')}\n"
    )
    return info



def get_document_info(ctx: RunContext[Dependencies], document_id: str) -> str:
    """
    Get information about a specific document by its ID.

    Args:
        ctx: RunContext with dependencies
        document_id: The ID of the document to get information about

    Returns:
        str: Information about the document or an error message
    """
    session = ctx.deps.session
    api_url = f"/integration/v2/document/{document_id}/"
    response = session.get(ctx.deps.al_base_url + api_url)
    response.raise_for_status()
    document = response.json()
    if not document:
        return f"No document found with ID '{document_id}'."
    
    info = (
        f"Document ID: {document.get('id', 'N/A')}\n"
        f"Title: {document.get('title', 'N/A')}\n"
        f"Description: {document.get('description', 'N/A')}\n"
        f"Owner: {document.get('owner', 'N/A')}\n"
        f"Created At: {document.get('created_at', 'N/A')}\n"
        f"Updated At: {document.get('updated_at', 'N/A')}\n"
    )
    return info


def get_schema_info(ctx: RunContext[Dependencies], schema_id: str) -> str:
    """
    Get information about a specific schema by its ID.

    Args:
        ctx: RunContext with dependencies
        schema_id: The ID of the schema to get information about

    Returns:
        str: Information about the schema or an error message
    """
    session = ctx.deps.session
    api_url = f"/integration/v2/schema/{schema_id}/"
    response = session.get(ctx.deps.al_base_url + api_url)
    response.raise_for_status()
    schema = response.json()
    if not schema:
        return f"No schema found with ID '{schema_id}'."
    
    info = (
        f"Schema ID: {schema.get('id', 'N/A')}\n"
        f"Name: {schema.get('name', 'N/A')}\n"
        f"Description: {schema.get('description', 'N/A')}\n"
        f"Owner: {schema.get('owner', 'N/A')}\n"
        f"Created At: {schema.get('created_at', 'N/A')}\n"
        f"Updated At: {schema.get('updated_at', 'N/A')}\n"
    )
    return info



def get_data_info(ctx: RunContext[Dependencies], data_id: str) -> str:
    """
    Get information about a specific data asset by its ID.

    Args:
        ctx: RunContext with dependencies
        data_id: The ID of the data asset to get information about

    Returns:
        str: Information about the data asset or an error message
    """
    session = ctx.deps.session
    api_url = f"/integration/v2/data/{data_id}/"
    response = session.get(ctx.deps.al_base_url + api_url)
    response.raise_for_status()
    data_asset = response.json()
    if not data_asset:
        return f"No data asset found with ID '{data_id}'."
    
    info = (
        f"Data Asset ID: {data_asset.get('id', 'N/A')}\n"
        f"Name: {data_asset.get('name', 'N/A')}\n"
        f"Description: {data_asset.get('description', 'N/A')}\n"
        f"Owner: {data_asset.get('owner', 'N/A')}\n"
        f"Created At: {data_asset.get('created_at', 'N/A')}\n"
        f"Updated At: {data_asset.get('updated_at', 'N/A')}\n"
    )
    return info

def get_key_from_object_info(object_info):
    key = None
    for info in object_info.split('\n'):
        info_key_value = info.split(':')
        if info_key_value[0] == "Column Key" or info_key_value[0] == "Table Key":
            key = info_key_value[1]
    return key    




# # Tools RDBMS
# - Tool for updating titles
# - Tool for updating descriptions


# # Good to have
# - Tool for bulk custom field updates