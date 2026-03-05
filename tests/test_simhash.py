from tracker.simhash import hamming_distance64, simhash64


def test_simhash_empty_is_zero():
    assert simhash64("") == 0


def test_simhash_small_change_has_small_distance():
    a = simhash64("hello world this is a test")
    b = simhash64("hello world this is test")
    assert hamming_distance64(a, b) <= 10

