package test5.project1;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

public class Reddit1
{
    public static void main( String[] args ) throws Exception
    {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "Reditt Submissions Hate Speech");
	    job.setJarByClass(Reddit1.class);
	    job.setMapperClass(RedditMapper.class);
	    job.setCombinerClass(RedditReducer.class);
	    job.setReducerClass(RedditReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setNumReduceTasks(20);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);  	    
    }

    static class RedditMapper extends Mapper<Object, Text, Text, IntWritable> {
	
		
		
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		String hatebase[] = {"abachabu", "abagima", "abaisia", "abazungu", "abbo",
					"ABC", "ABCD", "abd", "abeed", "abelungu", "abid", "abo", "Adolf", "af", "African catfish",
					"Africoon", "Afro-Saxon", "Ahab", "Ahmadiyah", "Ainu", "Ajam", "ajayee", "akata", "akhbaroshim",
					"alaman", "albino", "alligator bait", "allochtoon", "amakwerekwere", "ambattar", "ame koh",
					"Americoon", "AmeriKKKan", "Ami", "Amo", "anchor baby", "ang mor", "Angie", "Anglo", "Ann", "ape",
					"apple", "arabush", "aracuano", "arapis", "Argie", "Armo", "Arsch", "Arschgeburt", "Arschloch",
					"Asylschmarotzer", "Aunt Jane", "Aunt Jemima", "Aunt Mary", "Aunt Sally", "avalivavandu",
					"avaseeve", "azn", "azungu", "badugudugu", "bahadur", "bai tou", "baijo", "Baitola", "bak guiy",
					"balija", "baluba", "baluga", "bamboo coon", "banana", "banana bender", "banana lander", "Bangla",
					"banjo lips", "bans and cans", "basungu", "Bazi", "bazungu", "beach nigger", "bean dipper",
					"beaner", "beaner shnitzel", "beaney", "belegana", "Bengali", "berberaap", "bergie", "beur",
					"beurette", "bhrempti", "biba", "bicha", "bichinha", "bint", "bird", "bitch", "bitter clinger",
					"bix nood", "black Barbie", "black dago", "blanco", "blaxican", "blockhead", "bludger", "bluegum",
					"boche", "boffer", "bog", "bog hopper", "Bog Irish", "bog jumper", "bog trotter", "bogan", "bohunk",
					"boiola", "bolillo", "bong", "boo", "boofer", "boojie", "book book", "boon", "booner", "boong",
					"boonga", "boonie", "bor", "border bunny", "border hopper", "border jumper", "border nigger",
					"bosch", "bosche", "boskur", "bosyanju", "bougnoule", "Bounty bar", "boxhead", "bozgor",
					"brass ankle", "Braun", "brownie", "bubble", "buck", "buckethead", "buckra", "Buckwheat",
					"Buddhahead", "budos olah", "buffie", "bug eater", "bugre", "buk buk", "bule", "buleh",
					"Bulgaroskopian", "bumblebee", "bume", "bung", "bunga", "burrhead", "butterhead", "buzi",
					"cab nigger", "cabezas cuadradas", "cabezita negra", "caboclo", "caboco", "caco", "cacorro",
					"caffre", "cafuzo", "camel cowboy", "camel fucker", "camel humper", "camel jacker", "camel jockey",
					"can eater", "carcamano", "carpet pilot", "carrot snapper", "casado", "Caublasian", "cave nigger",
					"cefur", "celestial", "cerrero", "cetnik", "chach chach", "chakh chakh", "chalckiliyar", "chale",
					"champinon", "chan koro", "chankoro", "chapat", "chapata", "chapeton", "chapetona", "chapin",
					"chapina", "chapta", "Charlie", "charnega", "charnego", "charva", "charver", "chav", "chee chee",
					"cheena", "cheese eating surrender monkey", "chefur", "chekwa", "chele", "chelo", "chernozhopiy",
					"cheuchter", "chi chi", "chief", "chigger", "Chilango", "chili shitter", "Chinaman",
					"Chinese wetback", "Chinetoque", "ching chong", "chingeleshi", "chinig", "chinja chinja", "chink",
					"chink a billy", "chizungu", "chleuh", "cholero", "cholo", "chon", "chon koh", "chonky",
					"choochter", "chorik", "chosenjin", "Christ killer", "chuchter", "chug", "chunky", "chupta",
					"ciapak", "ciapata", "ciapaty", "Cioara", "cis male", "clam", "clamhead", "cocoa", "Cocoa Puff",
					"cocolo", "coconut", "colored", "coloured", "conspiracy theorist", "coolie", "coon", "coon ass",
					"cotton picker", "cow kisser", "cowboy killer", "cracker", "cripple", "crow", "crucco",
					"culona inchiavabile", "cultuurverrijker", "cunt", "curry muncher", "curry slurper",
					"curry stinker", "Cushi", "Cushite", "dago", "dambaya", "darkey", "darkie", "darky", "ddang kong",
					"dego", "demala", "dhimmi", "diaper head", "dinge", "dingo fucker", "dink", "disminuidos",
					"ditsoon", "dogan", "dogun", "dole bludger", "domes", "doryphore", "doss", "dot head", "dune coon",
					"dune nigger", "dyke", "dyke jumper", "egg", "eggplant", "eh hole", "eight ball", "el abeed",
					"emoit", "escuaca", "eurotrash", "eyetie", "fag", "faggot", "Fairy", "falang", "Fan Kuei", "fanook",
					"farang", "faranji", "fashisty", "fenucca", "fez", "Ficker", "filhos da terra", "Fin", "finook",
					"firangi", "FOB", "fog nigger", "Fotze", "four by two", "fresh off the boat", "frits", "fritsove",
					"Fritz", "frog", "Froschschenkelfresser", "fruit", "fryc", "fryce", "fuzzy", "fuzzy wuzzy",
					"gabacho", "gable", "gaijin", "gaiko", "gator bait", "Gaurkh", "geitenneuker", "gender bender",
					"Gerudo", "gew", "ghetto", "ghetto hamster", "ghost", "giaour ", "gin", "gin jockey", "ginger",
					"ginzo", "gipp", "gippo", "Glatze", "gokiburi", "golliwog", "goober", "gook", "gook eye", "gooky",
					"goomba", "goombah", "gora", "goy", "goyim", "goyum", "greaseball", "greaser", "gringa", "gringo",
					"groid", "guajiro ", "guala", "guala guala", "gub", "gubba", "guera", "guero", "guerro", "guido",
					"guinea", "guizi", "gummihals", "gun burglar", "gurrier", "gwailo", "Gwat", "gweilo", "gyp", "gypo",
					"gyppie", "gyppo", "gyppy", "haatmeneer", "hadji", "hagwei", "hairyback", "haji", "hajji",
					"half breed", "halfrican", "hamba", "hambaya", "han nichi", "Hans", "haole", "hapankaali", "hapshi",
					"hayquay", "hayseed", "hebe", "hebro", "heeb", "heinie", "Helga", "hick", "higger", "hillbilly",
					"Hitomodoki", "ho", "hoe", "homppeli", "honkey", "honkie", "honky", "Honyak", "Honyock", "hoosier",
					"hoser", "house nigger", "Hun", "Hunkie", "Hunky", "Hunni", "Hunyak", "Hunyock", "Hure",
					"Hurensohn", "hymie", "ice monkey", "ice nigger", "idiot", "ike", "ikey", "ikey mo", "ikizungu",
					"iky", "imeet", "impedido", "incapaz", "india", "indio", "indio ladino", "Indon", "injun",
					"Inselaffen", "intsik", "invalidos", "inyenzi", "island nigger", "ita koh", "jabonee", "jant",
					"Jap", "Japana", "japie", "Japse", "jareer", "jathi", "Jerry", "Jewbacca", "jhant", "jibaro", "jig",
					"jigaboo", "jigarooni", "jigg", "jigga", "jiggabo", "jiggaboo", "jigger", "Jihadi", "jijjiboo",
					"Jim Fish", "jincho", "Jincho papujo", "jock", "jockie", "jocky", "jokuoye", "judas", "Judensau",
					"Judenschwein", "jungle bunny", "Junior Mint", "kaaskop", "kabisi", "kabloonuk", "kaeriya",
					"kaffer", "kaffir", "kaffre", "kafir", "kafiri", "kala", "kalar", "kalla", "kallathoni",
					"kallathonni", "kalmuk", "kalmuki", "kamelenneuker", "Kanacke", "Kanaker", "kansarme", "karaiyar",
					"katol", "Katzenfresser", "keling", "khazar", "kihii", "kiingereza", "kijuju", "kike",
					"Kinderficker", "Kizungu", "Klandestin", "knacker", "kochchiya", "kokujin", "komunjara",
					"kopvoddrager", "kotiya", "kraut", "Krueppel", "kuffar", "kurombo", "kurwa", "Kushi", "Kushite",
					"kut-marokkaan", "kutmarokkaan", "kwai lo", "kwerekwere", "kweri kweri", "kwiri kwiri", "kyke",
					"labu", "lagartona", "laikci", "lamemurata", "Landya", "langsiya", "lansiya", "laowai", "Latin",
					"latrino", "lawn jockey", "Leb", "Lebbo", "lefty", "lemonhead", "leprechaun", "lesbo", "levis",
					"limey", "ling ling", "lofan", "longuu", "longuulkitkit", "lowlander", "lubra", "lugan",
					"Lugenpresse", "lungereza", "macaca", "Macaco", "macaroni", "Macedonist", "macengi", "mack",
					"mackerel snapper ", "maekka", "maessa", "Makaronifresser", "malaun", "maldito Bori", "malingsia",
					"mameluco", "mandilon", "mangia cake", "mangiacrauti", "mangiapatate", "mansplaining", "maricon",
					"marrano", "marron", "mattaya", "mayate", "medelander", "meiguo guizi", "melanzana", "melon",
					"Merkin", "mestico", "mestizo", "mezza fanook", "mick", "mickey", "Mickey Finn", "mil bag",
					"millie", "minusvalido", "Missgeburt", "mithangel", "moch", "mocho", "mock", "mockey", "mockie",
					"mocky", "mocro", "modaya", "moelander", "mof", "moffen", "mogolico", "moher", "mojado", "mojo",
					"moke", "moky", "mollo", "mong", "mongol", "mongolo", "monkey", "monser", "mook", "mooley",
					"mooliachi", "moolie", "moon cricket", "Moor", "moreno", "moro", "moss eater", "mosshead", "moulie",
					"moulignon", "moulinyan", "moxy", "mud duck", "mud person", "mud shark", "mudugudugu", "muk",
					"muktuk", "mulato", "mulignan", "mung", "munt", "munter", "murungu", "mustalainen", "mustalaiset",
					"musungu", "mutt", "muttal", "muzungu", "muzzie", "mwiji", "mzungu", "naca", "nacho", "naco",
					"Natsi", "Natzisty", "Nazi", "neche", "ned", "neechee", "neejee", "Neger", "negro",
					"negro de mierda", "Nemcurji", "Nemskutarji", "neres", "net head", "newfie", "ngetik", "nguoi tau",
					"nicca", "nichi", "nichiwa", "nidge", "niemra", "niemry", "nig", "nig nog", "nigar", "niger",
					"nigette", "nigga", "niggah", "niggar", "nigger", "nigglet", "niggor", "niggress", "nigguh",
					"niggur", "niglet", "nigor", "nigra", "nigre", "niknok", "niksmann", "nip", "nitchee", "nitchie",
					"nitchy", "Northern monkey", "ocker", "octaroon", "octroon", "ofay", "ola", "Orangie", "Oreo",
					"Oriental", "osuuji", "otuwa", "oven dodger", "paddy", "pakeha", "paki", "pakka", "paleface",
					"palla", "pancake", "pancake face", "papist", "papoose", "paraiyar", "parangi", "pato",
					"patriarchy", "payoponi", "peckerwood", "pedal", "peela", "pendatang", "Penner", "pepik", "Pepper",
					"Pepsi", "perra", "perroflauta", "pickaninny", "Piefke", "Piffke", "pig", "piker", "pikey", "piky",
					"pinche negro", "pindos", "pineapple nigger", "ping pang", "pinto", "plastic paddy", "pocha",
					"pocho", "poebe", "poep", "pogue", "pohm", "pointy head", "polack", "polacos", "Polake", "pollo",
					"pom", "pommie", "pommie grant", "pommy", "popolo", "poppadom", "porch monkey", "pot", "powderburn",
					"prairie nigger", "Preiss", "prieto", "prod", "proddy dog", "proddywhoddy", "proddywoddy",
					"property", "Prusak", "Prusatsi", "Pseudomacedonian", "Punjab", "pussy", "puta", "putain", "pute",
					"puto", "quadroon", "quashie", "queen", "queer", "race traitor", "raghead", "razakars", "red bone",
					"redlegs", "redneck", "redskin", "retard", "retarded", "retrasado mental", "Rhine monkey",
					"riben guizi", "ricepicker", "Rico Suave", "rifaap", "rifaapje", "rital", "roofucker", "rooinek",
					"rosbif", "rosuke", "roundeye", "rube", "rubwa", "ruco", "Russellite ", "rutuku", "Sack", "saeedi",
					"sakkiliya", "sakkiliyar", "saks", "salope", "sambo", "sand nigger", "Sapatao", "sart", "sauschwob",
					"SAWCSM", "sawney", "sayeedi", "sayoku", "scag", "scallie", "scally", "scanger", "scheiss Ami",
					"schiptar", "Schlampe", "Schlitzauge", "schvartse", "Schwanzlutscher", "schwartze", "schwarze",
					"schwarzer", "Schwuchtel", "scobe", "scuffer", "semihole", "senga", "seppo", "septic", "sesat",
					"shade", "shagitz", "shahktor", "shahktyer", "shant", "shanty Irish", "sheeny", "sheepfucker",
					"sheigetz", "sheister", "Shelta", "shemale", "shiksa", "shine", "shiner", "shit heel",
					"shit kicker", "shkutzim", "shvartz", "Shy", "Shylock", "shyster", "sideways cooter",
					"sideways pussy", "sideways vagina", "skag", "skanger", "skin", "skinhead", "skinny", "Skip",
					"Skippy", "Skopiana", "Skopianika", "Skopjan", "Skopjian", "slag", "slampa", "slant", "slant eye",
					"slave", "slit", "slope", "slopehead", "slopey", "slopy", "smick", "smoke jumper", "smoked Irish",
					"smoked Irishman", "snout", "snowflake", "sokac", "sokomokabul", "sole", "sooty", "soqi",
					"soup taker ", "Southern fairy", "spade", "Spast", "Spasti", "spear chucker", "sperg", "spic",
					"spice nigger", "spick", "spickaboo", "spide", "spig", "spigger", "spigotty", "spik", "spike",
					"spink", "spiv", "spook", "squarehead", "squaw", "squinty", "steek", "stovepipe", "stump jumper",
					"sub human", "sucker fish", "sudaca", "sudda", "suntan", "surrender monkey", "Svab", "Svaba",
					"szkop", "szwab", "tabeetsu", "Taffy", "Taig", "tan", "tapette", "tapori", "tar baby", "teabagger",
					"Teague", "teapot", "Teg", "Teig", "tek millet", "tenker", "tete carree", "teuchtar", "teuchter",
					"thalaya", "thambiya", "thicklips", "three fifth", "three fifths", "thurumba", "tiger",
					"timber nigger", "tincker", "tinkar", "tinkard", "tinker", "tinkere", "tizzone", "tizzoni", "Tommy",
					"touch of the tar brush", "towel head", "trailer park trash", "trailer trash", "tranny", "trash",
					"tree jumper", "trucin", "Tschusch", "tsekwa", "tugo", "tunnel digger", "Tunte", "twat", "Twinkie",
					"tyncar", "tynekere", "tynkard", "tynkare", "tynker", "tynkere", "Ubangee", "Ubangi", "umlungu",
					"umuzungu", "uncircumcised baboon", "uncivilised", "uncivilized", "Uncle Tom", "Untermensch",
					"Untermenschen", "uriti ja Mutigania wa wa Kunati", "ustasa", "uzko glaziye", "varungu",
					"Velcro head", "Viado", "vlah", "wagon burner", "WASP", "wazungu", "wetback", "wexican", "whigger",
					"Whipped", "white chocolate", "white nigger", "white privilege", "white trash", "whitey",
					"whore from Fife", "WIC", "Wichser", "wigga", "wigger", "wiggerette", "wink", "Wixer", "wog", "wop",
					"Yank", "Yankee", "yardie", "yellow", "yellow bone", "yid", "yob", "yobbo", "yokel", "yom", "youn",
					"yuon", "zainichi", "zambaggoa", "zambo", "zandneger", "zebra", "Zecke", "Ziegenficker", "zigabo",
					"zigeuner", "Zionazi", "zip", "zipperhead", "zippohead", "ZOG", "ZOG lover", "zorra" };
    		
    		String jsonString = value.toString();
			JSONObject obj = new JSONObject(jsonString);
			IntWritable one = new IntWritable(1);
			
			String selftext = obj.getString("selftext");
			
			String[] arr = selftext.split(" ");    

			 for ( String ss : arr) {
						 
				 if (Arrays.asList(hatebase).contains("ss.toLowerCase()")) {
					 Text outputKey = new Text(ss.toLowerCase());	
				 	 context.write(outputKey, one);
				 }
			  }
	
    	}
    	
    }

	static class RedditReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable word_count = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			Integer count = 0;
			
			for (IntWritable val : values) 
				count += val.get();
			
			word_count.set(count);
			context.write(key, word_count);
		}
	}    
}
