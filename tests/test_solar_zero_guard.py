import unittest


@unittest.skip('Obsolete after integrating solar.py into app.py live-power-only mode')
class SolarZeroGuardTests(unittest.TestCase):
    def test_obsolete(self):
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
