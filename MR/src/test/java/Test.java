import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lancer
 * @Date 2022/3/7 8:44 下午
 * @Description
 */
public class Test {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Bean implements Comparable<Integer> {
        private String name;
        private int age;

        @Override
        public String toString() {
            return "{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }

        @Override
        public int compareTo(Integer o) {
            return 0;
        }
    }

    public static void main(String[] args) {
    }
}
