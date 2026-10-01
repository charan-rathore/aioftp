import asyncio
import itertools
import pathlib

import pytest

import aioftp


@pytest.mark.asyncio
async def test_abort_stor(pair_factory):
    async with pair_factory() as pair:
        stream = await pair.client.upload_stream("test.txt")
        with pytest.raises((ConnectionResetError, BrokenPipeError)):
            while True:
                await stream.write(b"-" * aioftp.DEFAULT_BLOCK_SIZE)
                await pair.client.abort()


class SlowReadMemoryPathIO(aioftp.MemoryPathIO):
    async def read(self, *args, **kwargs):
        await asyncio.sleep(0.01)
        return await super().read(*args, **kwargs)


@pytest.mark.asyncio
async def test_abort_retr(pair_factory, Server):
    s = Server(path_io_factory=SlowReadMemoryPathIO)
    async with pair_factory(None, s) as pair:
        await pair.make_server_files("test.txt")
        stream = await pair.client.download_stream("test.txt")
        for i in itertools.count():
            data = await stream.read(aioftp.DEFAULT_BLOCK_SIZE)
            if not data:
                stream.close()
                break
            if i == 0:
                await pair.client.abort()


@pytest.mark.asyncio
async def test_abort_retr_no_wait(
    pair_factory,
    Server,
    expect_codes_in_exception,
):
    s = Server(path_io_factory=SlowReadMemoryPathIO)
    async with pair_factory(None, s) as pair:
        await pair.make_server_files("test.txt")
        stream = await pair.client.download_stream("test.txt")
        with expect_codes_in_exception("426"):
            for i in itertools.count():
                data = await stream.read(aioftp.DEFAULT_BLOCK_SIZE)
                if not data:
                    await stream.finish()
                    break
                if i == 0:
                    await pair.client.abort(wait=False)


@pytest.mark.asyncio
async def test_nothing_to_abort(pair_factory):
    async with pair_factory() as pair:
        await pair.client.abort()


class SlowListMemoryPathIO(aioftp.MemoryPathIO):
    async def is_file(self, *a, **kw):
        return True

    def list(self, *args, **kwargs):
        class Lister(aioftp.AbstractAsyncLister):
            async def __anext__(cls):
                await asyncio.sleep(0.01)
                return pathlib.PurePath("/test.txt")

        return Lister()

    async def stat(self, *a, **kw):
        class Stat:
            st_size = 0
            st_mtime = 0
            st_ctime = 0

        return Stat


@pytest.mark.asyncio
async def test_mlsd_abort(pair_factory, Server):
    s = Server(path_io_factory=SlowListMemoryPathIO)
    async with pair_factory(None, s) as pair:
        cwd = await pair.client.get_current_directory()
        assert cwd == pathlib.PurePosixPath("/")
        async for path, info in pair.client.list():
            await pair.client.abort()
            break


@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["LIST", "MLSD", "RETR test.txt", "STOR upload.txt", "APPE upload.txt"])
async def test_abort_before_data_connection(pair_factory, command):
    async with pair_factory(do_quit=False) as pair:
        await pair.make_server_files("test.txt")
        await pair.client.command("EPSV", "229")
        await pair.client.command(command, "150")
        connection = next(iter(pair.server.connections.values()))
        data_connection = connection["data_connection"]
        assert not data_connection.done()
        await pair.client.command("ABOR")
        code, _ = await asyncio.wait_for(pair.client.parse_response(), 0.2)
        assert str(code) == "426"
        code, _ = await asyncio.wait_for(pair.client.parse_response(), 0.2)
        assert str(code) == "226"
        # The cancelled transfer must not poison readiness for a later transfer.
        assert not data_connection.cancelled()
        async with pair.client.download_stream("test.txt") as stream:
            assert await stream.read() == b"-" * aioftp.DEFAULT_BLOCK_SIZE * 3
        await pair.client.quit()


@pytest.mark.asyncio
async def test_data_connection_wait_timeout(pair_factory, Server):
    async with pair_factory(None, Server(wait_future_timeout=0.02)) as pair:
        await pair.make_server_files("test.txt")
        await pair.client.command("EPSV", "229")
        await pair.client.command("RETR test.txt", "150")
        connection = next(iter(pair.server.connections.values()))
        data_connection = connection["data_connection"]
        await pair.client.command(None, "425")
        assert not data_connection.cancelled()
        async with pair.client.download_stream("test.txt") as stream:
            assert await stream.read() == b"-" * aioftp.DEFAULT_BLOCK_SIZE * 3
