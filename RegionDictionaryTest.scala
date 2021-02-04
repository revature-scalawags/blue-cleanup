import org.scalatest.funsuite.AnyFunSuite

class RegionDictionaryTest extends AnyFunSuite {
  // Test that countries show up in their correct region
  test("\"Germany\" should show up as \"Europe\"") {
    assert(RegionDictionary.reverseMapSearch("Germany") == "Europe")
  }

  // Test that countries that don't exist, return Country Not Found
  test("\"Atlantis\" should show up as \"Country Not Found\"") {
    assert(RegionDictionary.reverseMapSearch("Atlantis") == "Country Not Found")
  }
}