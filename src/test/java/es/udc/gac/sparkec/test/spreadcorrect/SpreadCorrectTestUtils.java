
package es.udc.gac.sparkec.test.spreadcorrect;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import es.udc.gac.sparkec.spreadcorrect.Recommendation;


public class SpreadCorrectTestUtils {
    public static List<Recommendation> decodeRecommend(String encoded){
        List<Recommendation> result = new LinkedList<>();
        int j = 0;
        for (int i = 0; i < encoded.length(); i++){
            char c = encoded.charAt(i);
            if (c == 'X'){
                j++;
                continue;
            }
            
            result.add(
                new Recommendation(
                    c,
                    j
                )
            );
        }
        return result;
    }
    
    public static String encodeRecommend(List<Recommendation> recommend){
        recommend.sort((Recommendation a, Recommendation b) -> {

            if (a.getPos() < b.getPos()){
                return -1;
            } else if (a.getPos() == b.getPos()){
                return 0;
            } else {
                return 1;
            }
        });
        StringBuilder sb = new StringBuilder();
        StringBuilder aux = new StringBuilder();
        int i = 0;
        Iterator<Recommendation> it = recommend.iterator();
        while(it.hasNext()){
            Recommendation r = it.next();
            while (i < r.getPos()){
                i++;
                if (aux.length() > 0 ){
                    char[] charData = aux.toString().toCharArray();
                    Arrays.sort(charData);
                    sb.append(charData);
                    aux = new StringBuilder();
                }
                sb.append('X');
            }
            aux.append(r.getBase());
        }
        
        return sb.toString();
    }
}
