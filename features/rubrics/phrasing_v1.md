# Phrasing-risk rubric (prompt_version: phrasing_v1)

Context: Kalshi "mentions" markets settle YES if a company's earnings-call speakers say a listed word.
Under Kalshi's rules these DO count automatically (don't list them as alternatives):
- the exact word or phrase, and any "/"-separated alternative in the listing
- plurals (-s, -es, -y -> -ies) and possessives
- spacing/hyphen variants ("buy back" = "buyback"), accents, "+" vs "plus"
- any longer phrase that contains the word ("tariff" counts inside "tariff headwinds")

Other inflections do NOT count ("tariffed", "tariffing", "innovate" for "innovation").

## Task
For each (company, listed word) pair, list the **alternatives**: other words or phrases a speaker on
THIS company's call would naturally use to refer to the SAME topic, that would NOT settle the market.

Include:
- synonyms and near-synonyms used in business speech ("duties", "levies" for Tariff)
- acronym vs. spelled-out form when only one is listed ("AI" when only "Artificial Intelligence" is listed)
- non-counting inflections or derived forms that carry the same meaning ("tariffed", "innovate")
- product, brand, program or person names that commonly stand in for the topic on this company's calls
- for a person: other ways they're referred to (title only, first name only) when that is common

Exclude:
- anything that contains the listed word (it would count)
- broader or related topics that are not interchangeable (for "Tariff", "trade policy" is borderline: include
  only if speakers routinely use it INSTEAD of the word to mean the same thing)
- made-up or rare phrasings. Only list things a real executive or analyst would plausibly say.

Order alternatives from most to least likely. Maximum 12. Use lowercase unless it's a proper name.

## Fields per pair
- `alternatives`: list of strings (can be empty)
- `n_alternatives`: length of that list
- `phrasing_risk`: 0, 1 or 2
  - 0 = essentially one way to say it (proper names, distinct jargon, unique products). Alternatives empty or very rare.
  - 1 = some alternatives exist, but the listed word is the standard way this company refers to the topic.
  - 2 = the topic is commonly referenced in other ways; the listed word is one of several equally natural options,
        or speakers often talk around it.
- `confidence`: "high" or "low"
- `note`: under 100 characters, only when confidence is low

Judge each pair for THAT company and its industry. Do not use any knowledge of whether the word was
actually said on any specific call; this is purely about language.
