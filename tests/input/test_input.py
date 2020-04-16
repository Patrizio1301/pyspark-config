from main.process.input.sources.source import Source

def test_sum():
    assert sum([1, 2, 3]) == 6

def test_source():
    print(Source.get_from_config())
    True


