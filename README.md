#  FastAPI Boilerplate with SQLAlchemy Integration

This project demonstrates how to use FastAPI with JWT for authentication, Argon2 for password hashing, and SQLAlchemy for database ORM. The application includes endpoints for user login, signup, account update, and account deletion.


## Prerequisites

- Python 3.11+
- Docker & Docker Compose (Recommended)
- PostgreSQL (Production) or SQLite (Development)

## Setup with Docker (Recommended)

1. **Clone the repository**
2. **Configure your environment**
   Create a `.env` file based on the template below:
   ```env
   SECRET_KEY=your_very_secret_key_at_least_32_chars
   DATABASE_PASSWORD=your_db_password
   ALLOWED_ORIGINS=http://localhost:3000,https://yourdomain.com
   ```
3. **Run with Docker Compose**
   ```bash
   docker-compose up -d
   ```

## Local Development Setup
## 2. Create a .env file

```env
SECRET_KEY=" "
ALGORITHM="HS256"
ACCESS_TOKEN_EXPIRE_MINUTES=790
database_paths="databases" 
database_names="test.db"
admin_paswd="Admin@123456"
log_filepath="./logs/"
memory_costs=35555
pararellisms=1
hash_length=322
salt_length=16
```

- `SECRET_KEY:` Your secret key for encoding JWT tokens.
- `ALGORITHM:` The algorithm used for encoding JWT tokens.
- `ACCESS_TOKEN_EXPIRE_MINUTES:1 The expiration time for access     tokens in minutes.
- `databases_path:` Path to your database file.
- `databases_name:` Name to your database file like test.db for sqlite3. e.g. `talha.db`
- `ADMIN_PASSWORD:` Default password for the admin user.
- `LOG_FILEPATH:` Path for log file storage.
- “`memory_costs`, `pararellisms`, `hash_length`, `salt_length`: Parameters for Argon2 config”
- `SALT:` additional to secure password
- `PEPPER` additional to secure password
## Database Setup
Ensure your database schema is set up correctly. This application assumes you have a users table with the following columns:

- **`name`**
- **`email`**
- **`password`**
- **`profile_pic`**
- **`user_role`**
- **`disabled`**

### Database Configuration
The application is pre-configured for PostgreSQL with `asyncpg`. 

1. Ensure your `.env` contains the correct `DATABASE_*` credentials.
2. Apply migrations using Alembic:
   ```bash
   alembic upgrade head
   ```
3. Modify the ```SQLAlchemy``` database URL in your FastAPI app in ``` App/GetEnvDate.py``` configuration:
   ```python
    DATABASE_URL = os.getenv("DATABASE_URL")

   ```
## Endpoints
### Login
- **URL:** `/login`
**Method:** `POST`
**Description:** `Login with username and password.`
**Query Parameters:**
**username:** `The username of the account.`
**password:** `The password of the account.`
### **Responses:**
**`200 OK:`** Returns an access token.
**`401 Unauthorized:`** Invalid username or password.
### **Signup**
- **URL:** `/signup`
- **Method:** `POST`
- **Description:** Create a new user account.
```json
{
  "name": "string",
  "email": "string",
  "password": "string",
  "profile_pic": "string",
  "disable": boolean
}

```
##  Responses:
- **200 OK:** Account created successfully.
- **500 Internal Server Error:** Failed to sign up due to server error.

## Update Account
- **URL:** /update_acount
- **Method:** PATCH
- **Description:** Update user account details.
- **Request Body**
```json
{
  "name": "string",
  "email": "string",
  "password": "string",
  "profile_pic": "string",
  "user_role": "string",
  "disable": boolean
}

```


- **Query Parameters:**
**`user_id:`** ID of the user to update (admin only).
- **`Responses:`**
- **`200 OK:`** Account updated successfully.
- **`400 Bad Request:`** No update fields provided.
- **`404 Not Found:`** User not found.
- **`500 Internal Server Error:`** Error updating account.
### Delete Account
- **URL: /delete_account**
- **Method: DELETE**
- **`Description: Delete user account.**
- **`Query Parameters:**
- **`uid: ID of the user to delete (admin only).**
- **`password: Password of the account (for non-admin - users).`**
- ### Responses:
- ***`200 OK:`*** Account deleted successfully.
- ***`400 Bad Request:`*** Invalid request parameters.
- ***`401 Unauthorized:`*** Invalid token or password.
- ***`404 Not Found:`*** User not found.
- ***`500 Internal Server Error:`*** Error deleting account.

### Running the Application

To run the FastAPI application, use Uvicorn:

```bash
uvicorn main:app --reload
```
Replace `main` with the name of your Python file if it's different.D

### Logging
Logs are stored in the directory specified by **`LOG_FILEPATH`** in the **`.env`** file.
### Security
- Passwords are hashed using Argon2.
- JWT tokens are used for authentication and have an expiration time.
### Notes
- Ensure to replace placeholder values in the .env file with your actual configuration.
- Update database paths and configurations according to your environment.
 
### Project Information
This is a private project named iotNoob by Talha.