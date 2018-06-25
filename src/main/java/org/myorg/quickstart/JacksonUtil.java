package org.myorg.quickstart; /**
 *
 */

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by wangzhongfu on 2017/5/5.
 */
public class JacksonUtil {

    private static Logger logger = LoggerFactory.getLogger(JacksonUtil.class);

    public static ObjectMapper objectMapper = new ObjectMapper();
    private static final ObjectMapper SORTED_MAPPER = new ObjectMapper();

    static {
        objectMapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
        SORTED_MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    /**
     * 使用泛型方法，把json字符串转换为相应的JavaBean对象。
     * (1)转换为普通JavaBean：readValue(json,Student.class)
     * (2)转换为List,如List<Student>,将第二个参数传递为Student
     * [].class.然后使用Arrays.asList();方法把得到的数组转换为特定类型的List
     *
     * @param jsonStr
     * @param valueType
     * @return
     */
    public static <T> T readValue(String jsonStr, Class<T> valueType) throws Exception {
        return objectMapper.readValue(jsonStr, valueType);
    }

    /**
     * json数组转List
     *
     * @param jsonStr
     * @param valueTypeRef
     * @return
     */
    public static <T> T readValue(String jsonStr, TypeReference<T> valueTypeRef) throws Exception {
        return objectMapper.readValue(jsonStr, valueTypeRef);
    }

    /**
     * 把JavaBean转换为json字符串
     *
     * @param object
     * @return
     */
    public static String toJSon(Object object) throws Exception {
        return objectMapper.writeValueAsString(object);
    }

    public static JsonNode toJsonNode(String json) throws IOException {
        return objectMapper.readTree(json);
    }

    public static <T> T convertValue(Object o, Class<T> valueType) {
        return objectMapper.convertValue(o, valueType);
    }

    /**
     * 字段按照字母表排序的json
     *
     * @param object
     * @return
     */
    public static String toSortedJson(Object object) throws Exception {
        JsonNode jsonNode = convertValue(object, JsonNode.class);
        Object obj = SORTED_MAPPER.treeToValue(jsonNode, Object.class);
        return SORTED_MAPPER.writeValueAsString(obj);
    }

}
