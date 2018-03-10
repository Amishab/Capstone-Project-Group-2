package AnalyticsProcessor;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.SentenceUtils;
import edu.stanford.nlp.process.DocumentPreprocessor;
import utils.replace_UTF8;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;


public class ArticleCleaner {
    String article;
    String senators_csv = "data/senate_w_LN.csv";
    ArrayList<SenatorClass> senators;
    List<String> sentenceList = new ArrayList<String>();

    private class SenatorClass {
        String name;
        String lastName;
        int y_i_o;
        String party;
        String state;
        int termEnds;

        SenatorClass(String sname, String slastName, int sy_i_o, String sparty, String sstate, int stermEnds){
            name=sname;
            lastName = slastName;
            y_i_o = sy_i_o;
            party = sparty;
            state = sstate;
            termEnds = stermEnds;
        }
    }

    ArticleCleaner(String text) {
        this.article = text;
        senators = new ArrayList<SenatorClass>();
        try {

            readCSV(senators_csv);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readCSV(String filePath) throws IOException{
        try {
            Reader reader = Files.newBufferedReader(Paths.get(filePath));
            CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build();

            String[] st;
            while ((st = csvReader.readNext()) != null) {
                this.senators.add(new SenatorClass(st[0],st[1],Integer.parseInt(st[2]),st[3],st[4],Integer.parseInt(st[5])));
            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    void replaceIllegalChars() {
        String text = article;

        try {
            text = replace_UTF8.ReplaceLooklike(text);
            text = text.replace("≦", "<=");
            text = text.replace("≧", ">=");
            text = text.replace("㎡", "m2");
            text = text.replace("ï", "i");
            text = text.replace("˄", "^");
            text = text.replace("˚", " degrees");
            text = text.replace("※", "-");
            text = text.replace("㎲", " microseconds");
            text = text.replace("́s", "'s");
            text = text.replace("╳", "x");
        } catch (IOException e) {
            e.printStackTrace();
        }

//        text = text.replace(" - ", "\n\n");
//        text = text.replace("- ", "\n\n");

        article = text.trim();
    }

    void unescapeUnicode() {
        String text = article;
        if (text.contains("\\u")) {
            text = text.replaceAll(Pattern.quote("\\u0091"), "‘");
            text = text.replaceAll(Pattern.quote("\\u0092"), "’");
            text = text.replaceAll(Pattern.quote("\\u0093"), "“");
            text = text.replaceAll(Pattern.quote("\\u0094"), "”");
            text = text.replaceAll(Pattern.quote("\\u0095"), "•");
            text = text.replaceAll(Pattern.quote("\\u0096"), "–");
            text = text.replaceAll(Pattern.quote("\\u0097"), "—");
            text = text.replaceAll(Pattern.quote("\\u0098"), "˜");
            text = text.replaceAll(Pattern.quote("\\u0099"), "™");
            text = text.replaceAll(Pattern.quote("\\u009A"), "š");
            text = text.replaceAll(Pattern.quote("\\u009B"), "›");
            text = text.replaceAll(Pattern.quote("\\u009C"), "œ");
            text = text.replaceAll(Pattern.quote("\\u009D"), "");
            text = text.replaceAll(Pattern.quote("\\u009E"), "ž");
            text = text.replaceAll(Pattern.quote("\\u009F"), "Ÿ");
        }

        article = text;
    }
    void buildSentenceList()
    {
        Reader reader = new StringReader(article);
        DocumentPreprocessor dp = new DocumentPreprocessor(reader);

        for (List<HasWord> sentence : dp) {
            // SentenceUtils not Sentence
            String sentenceString = SentenceUtils.listToString(sentence);
            this.sentenceList.add(sentenceString);
        }
    }

    String clean()   {
        StringBuilder filteredArticles = new StringBuilder();

        unescapeUnicode();
        replaceIllegalChars();
        buildSentenceList();

        for (String sentence : sentenceList) {
            for (SenatorClass senator: senators) {
                if (sentence.contains(senator.name) || sentence.contains(senator.lastName)) {
                    filteredArticles.append(sentence);
                    break;
                }
            }
        }

        return filteredArticles.toString();
    }


    public static void main(String args[]) throws IOException {
        System.out.println("ArticleCleaner Start: ");

        String article = "SACRAMENTO, Calif. (AP) \\u0097 California's Democratically-controlled Legislature kicked off 2017 pledging to stand strong against Republican President Donald Trump and pursue a liberal slate of policies on everything from climate change to health care.\\n\\nThey headed home Friday for the year having reauthorized a major climate change-fighting initiative and hiking taxes to pay for road and bridge repairs. But a proposal to provide universal health care coverage for Californians fell by the wayside.\\n\\nDemocratic Gov. Jerry Brown has until Oct. 15 to sign legislation.\\n\\nHere's a look at what lawmakers did \\u0097 or didn't do \\u0097 this year.\\n\\nAdvertisement\\n\\nENVIRONMENT\\n\\nLawmakers gave another decade of life to California's cap-and-trade program, the centerpiece of the state's effort to curb greenhouse gas emissions.\\n\\nThe measure passed with bipartisan support, ultimately costing Assembly Republican Leader Chad Mayes his job following an insurrection from party activists. Environmental justice groups, meanwhile, argued it was too generous to oil companies. They weren't mollified by companion legislation to address toxic air around oil refineries.\\n\\nCap and trade puts limit on carbon emissions and requires polluters to obtain permits to release greenhouse gases under the cap. Some permits are auctioned off, providing billions of dollars in state revenue. Lawmakers voted to spend $1.5 billion of that money on electric vehicle rebates, cleaner trucks and buses and other initiatives to reduce pollution.\\n\\nROAD REPAIRS \\u0097 AND A TAX HIKE\\n\\nAfter years of failed attempts to address much-needed road repairs across California, lawmakers voted to increase the gas tax to generate $5 billion a year.\\n\\nGas prices will rise 12 cents per gallon in November and 19.5 cents by 2020. Diesel taxes will increase 20 cents, and drivers will pay a new vehicle registration fee ranging from $25 to $175 depending on the value of their vehicles.\\n\\nThe money pays primarily for road repairs, not new or expanded highways, though some of it will also fund transit, parks and other projects.\\n\\nThe tax increase is at the center of an attempt by Republicans to recall Democratic Sen. Josh Newman, who supported it.\\n\\nHEALTH CARE\\n\\nAssembly Speaker Anthony Rendon, D-Paramount, faced a backlash when he shelved a plan to eliminate health insurance coverage and provide government-funded health care for everyone in California.\\n\\nRendon said the proposal, which did not have a plan to raise the estimated $400 billion per year that it would cost, was not fully developed. He's since formed a special committee to explore universal health coverage.\\n\\nMeanwhile, the Legislature approved a drug-price transparency bill that had stalled for two years. It requires pharmaceutical companies to provide advance notice before instituting large price increases. The bill is awaiting a signature or veto from Brown.\\n\\nLAW ENFORCEMENT\\n\\nLawmakers failed to act on one of their highest-profile law enforcement issues: changing a money bail system that critics say disproportionately punishes poor defendants. They plan to try again next year.\\n\\nBut the Legislature did send Brown two bills that would lighten criminal penalties for young offenders. One would bar sentencing juveniles to life without parole, in keeping with recent U.S. Supreme Court rulings. The other would expand the state's youthful parole program by requiring parole consideration for offenders who committed their crimes before age 25, up from age 23 in current law.\\n\\nAnother would write into state law a federal court order that requires officials to consider releasing inmates age 60 or older who have served at least 25 years in prison, excluding death row and other no-parole inmates along with police killers and third-strike career criminals.\\n\\nOther bills would restrict employers from asking about prior criminal convictions on job applications, allow juvenile offenders to ask a judge to seal records of crimes committed before turning 17 and end additional three-year sentence for repeat drug offenders.\\n\\nIMMIGRATION\\n\\nThe centerpiece of California's effort to push back against Trump's immigration policies is a plan to limit cooperation of local and state law enforcement with federal immigration agents. Dubbed a \\\"sanctuary state\\\" proposal, it passed the Legislature late Friday and heads to Brown for signature.\\n\\nUnder other immigration bills awaiting Brown's action, immigration officials would need warrants to enter college campuses or work places. And lawmakers approved $30 million in legal and college financial aid for participants in a program that gives temporary legal protection to young immigrants brought to the United States illegally as children or by parents who overstayed visas.\\n\\nHOUSING\\n\\nA package of bills to address the state's affordable housing crisis likewise cleared the Legislature at the last minute, with Brown planning to sign them. The three key bills will place a $4 billion housing bond on the 2018 ballot, streamline development approval in communities that aren't meeting housing goals, and add a $75 fee on real estate transaction documents.\\n\\nCalifornia is home to many of the nation's most expensive rental markets, and demand for housing outpaces supply by about 1.5 million homes.\\n\\nOTHERS\\n\\nPeople who don't pay traffic fines will no longer face a suspension of their driver's license under a budget bill signed by Brown. That's one of a handful of bills passed this year aimed at eliminating fines that disproportionately harm poor people.\\n\\nA bill headed to Brown would let Californians choose the third gender option of \\\"non-binary\\\" on driver's licenses and other state documents. Lawmakers also sent the governor a bill to ban pet stores from selling dogs, cats and rabbits bred at mass-breeding operations, instead requiring them to work with animal shelters and rescue operations.\\n\\nAn effort to push back middle and high school start times to 8:30 a.m. or later also failed to pass, but will be addressed next year.\\n\\n___\\n\\nAssociated Press writer Kathleen Ronayne contributed reporting.";
        ArticleCleaner ac = new ArticleCleaner(article);

        System.out.println(ac.clean());
    }
}
