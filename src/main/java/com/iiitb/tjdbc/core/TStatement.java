package com.iiitb.tjdbc.core;

import com.sun.xml.internal.ws.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public class TStatement {

    private TJdbc tJdbc;

    public String processQuery(String query) {
        System.out.println("Recieved query for processing: " + query);

        //contains tokens in the original query string
        List<String> tokens = new ArrayList<>();
        //key = keyword, value = index of keyword in tokens list
        Map<String, Integer> keywordPositionMap = new HashMap<>();
        tJdbc = new TJdbc();
        tokens = tokenize(query, tokens, keywordPositionMap);

        if (keywordPositionMap.containsKey(TJdbc.TEMPORALIZE)) {

        } else if (keywordPositionMap.containsKey(TJdbc.FIRST)) {

        }

        return query;
    }

    private List<String> tokenize(String query, List<String> tokens, Map<String, Integer> keywordPositionMap) {
        if (Objects.nonNull(query)) {
            tokens.addAll(Arrays.asList(query.split("\\s+")));
            tokens = tokens.stream().map(String::toLowerCase).collect(Collectors.toList());
            for (int i = 0; i < tokens.size(); i++) {
                if (tJdbc.getKeywords().contains(tokens.get(i))) {
                    keywordPositionMap.put(tokens.get(i), i);
                }
            }
        }
        return tokens;
    }
}
