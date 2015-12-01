

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class AFINNScoreCalculator implements Serializable {
	public static final long serialVersionUID = 42L;
	private static AFINNScoreCalculator _singleton;
	private Map<String, Integer> wordScore;
	
	// constructor
	private AFINNScoreCalculator() {
		this.wordScore = new HashMap<String, Integer>();
		BufferedReader rd = null;
		rd = new BufferedReader(
				new InputStreamReader(
					this.getClass().getResourceAsStream("/sentiment_analysis/AFINN-111.txt")));
		
		String line;
		try {
			while ((line = rd.readLine()) != null){
				List<String> word_score = Arrays.asList(line.split("\t"));
				this.wordScore.put(word_score.get(0), Integer.parseInt(word_score.get(1)));
			}
		} catch (IOException ex) {
			Logger.getLogger(this.getClass()).error("IO error while initializing", ex);
		} finally {
            try {
                if (rd != null) rd.close();
            } catch (IOException ex) {
            	Logger.getLogger(this.getClass()).error("IO error while initializing", ex);
            }
        }
	}

	// implement singleton
	private static AFINNScoreCalculator get(){
		if(_singleton == null)
			_singleton = new AFINNScoreCalculator();
		return _singleton;
	}
	
	public static Map<String, Integer> getAFINNScoreCalculator(){
		return get().wordScore;
	}
}
