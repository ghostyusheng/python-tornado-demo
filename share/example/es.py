# move this file to eng-server for use demo
from share.repository.els import ElsRepository
import asyncio


async def main():
    await ElsRepository(['192.168.127.12:9200']).initIndex('user_center')
    #es = ElsRepository(['39.106.14.18:9200'])
    #await es.upsert('wide_video', 1,{
    #    'app_id': 10,
    #    'comments': 10
    #})
    #demo = await es.getById('wide_video', 1)
    #print(demo)


if __name__ == '__main__':
    asyncio.run(main())

