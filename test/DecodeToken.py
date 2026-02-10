from App.api.dependencies.auth import   decode_jwt
import asyncio
from App.core.Connector import get_db,database

if __name__=="__main__":
    try:
        tok="""eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0YWxoYUBwb3N0ZS50YWxoYS5wayIsInVzZXJfaWQiOjUsImV4cCI6MTc3MDczNDY5NCwiaWF0IjoxNzcwMTI5ODk0LCJ0eXBlIjoicmVmcmVzaCJ9.FQTN82Hsl_ZZbui04qtzYKADULt3Hk_mekxacl4vlN0"""
        database.connect()
        cdb=get_db()
        # vt=asyncio.run(auth.get_current_user(tok,db=cdb))
        # print(vt)
        rt=decode_jwt(tok)
        print(rt)
    except Exception as e:
        print(f"Error decode token due to {e}")