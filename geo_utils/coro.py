"""
Tools for building coroutine pipelines
"""
from typing import Coroutine, TypeVar, List
import asyncio
import aiohttp
import aiofiles


async def abatch(in_queue: asyncio.Queue, out_queue: asyncio.Queue, max_size: int = 50, timeout: float = 1):
    batch = []
    while True:
        should_submit = False
        try:
            next_val = await asyncio.wait_for(in_queue.get(), timeout=timeout)
            batch.append(next_val)
        except asyncio.TimeoutError:
            if not batch:
                # The queue seems to be empty and so is our batch.
                # Likely delay upstream so give it a moment
                await asyncio.sleep(0.1)
            else:
                # Queue is empty and we have stuff so go ahead and submit
                should_submit = True

        if should_submit or len(batch) >= max_size:
            await out_queue.put(batch)
            for _ in range(len(batch)):
                in_queue.task_done()
            batch = []


async def abatch_geocoder(in_queue: asyncio.Queue, success_queue: asyncio.Queue, failure_queue: asyncio.Queue, max_retries: int = 3):
    # Create a TCP/IP session to avoid setup time
    async with aiohttp.ClientSession() as session:
        while True:
            # Get a batch from the input queue
            batch = await in_queue.get()

            # Prep it for this particular geocoder
            prepped_batch = prep_batch(batch)

            # Sometimes TCP/IP has issues; accept we may need some retries on certain error codes
            success = False
            for _ in range(max_retries):
                # Submit to the geocoder
                async with session.post(GEOCODER_URL, json=prepped_batch) as response:

                    # If the error is retriable, just go another round
                    if response.retriable_error:
                        continue

                    # An error that indicates we should just give up :-(
                    if response.fatal_error:
                        for value in batch:
                            await failure_queue.put(value)
                        in_queue.task_done()
                        success = True
                        break

                    # We got responses! Parse them into successes and failures
                    for geocoded in process_response(response):
                        if geocoded.is_match():
                            await success_queue.put(geocoded)
                        else:
                            await failure_queue.put(geocoded)

                    success = True
                    in_queue.task_done()
                    break

            if not success:
                # Made it through all the retries. Give up
                for value in batch:
                    await failure_queue.put(value)
                in_queue.task_done()


async def source(input_filename: str, out_queue: asyncio.Queue):
    async with aiofiles.open(input_filename, 'rt') as infile:
        async for line in infile:
            await out_queue.put(process_line(line))


async def sink(output_filename: str, in_queue: asyncio.Queue):
    async with aiofiles.open(output_filename, 'wt') as outfile:
        while True:
            val = await in_queue.get()
            await outfile.write(process_val(val))
            in_queue.task_done()


async def pipeline(input_filename: str, output_filename: str):
    queues: List[asyncio.Queue] = []
    tasks: List[asyncio.Task] = []

    # Setup source; DO NOT add it to tasks, which will be cancelled later
    source_queue = asyncio.Queue()
    queues.append(source_queue)
    reader = source(input_filename, source_queue)

    # Setup sink. This operates as a worker, so add it to tasks to be cancelled
    sink_queue = asyncio.Queue()
    queues.append(sink_queue)
    writer = asyncio.create_task(sink(output_filename, sink_queue))
    tasks.append(writer)

    # Setup first geocoder and hook it up to a batcher
    first_geocoder_queue = asyncio.Queue()
    second_geocoder_batch_queue = asyncio.Queue()
    batcher = asyncio.create_task(abatch(source_queue, first_geocoder_queue))
    first_geocoder = asyncio.create_task(abatch_geocoder(first_geocoder_queue, sink_queue, second_geocoder_batch_queue))

    tasks.append(batcher)
    tasks.append(first_geocoder)
    queues.append(first_geocoder_queue)
    queues.append(second_geocoder_batch_queue)

    # Setup second (and here, last) geocoder
    second_geocoder_in_queue = asyncio.Queue()
    batcher = asyncio.create_task(abatch(second_geocoder_batch_queue, second_geocoder_in_queue))
    second_geocoder = asyncio.create_task(abatch_geocoder(second_geocoder_in_queue, sink_queue, sink_queue))

    tasks.append(batcher)
    tasks.append(second_geocoder)
    queues.append(second_geocoder_in_queue)

    # Explicitly await the reader, which returns when the file is read
    await reader

    # Then wait for all the queues to drain
    # Since we've awaited the reader, it is impossible for them all to be done
    # and not have processed the file
    await asyncio.gather(*[queue.join() for queue in queues])

    # Finally, since we're done, cancel all of our workers, which are while True-ing
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
