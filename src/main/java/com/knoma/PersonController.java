package com.knoma;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.knoma.pojo.Person;
import com.knoma.pojo.PersonDAO;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import jakarta.inject.Inject;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.UUID;

@Controller("/person")
public class PersonController {

    private PersonDAO personDAO;

    @Inject
    public PersonController(CqlSession session) {
        PersonMapper personMapper = new PersonMapperBuilder(session).build();
        this.personDAO = personMapper.personDao(CqlIdentifier.fromCql("cass_drop"));
    }

    @Get(uri = "/{personId}", produces = MediaType.APPLICATION_JSON)
    public Mono<Person> get(UUID personId) {
        return Mono.fromCompletionStage(personDAO.getById(personId))
                .map(person -> person);
    }

    @Delete(uri = "/{personId}", produces = MediaType.APPLICATION_JSON)
    public Mono<Map<String, Long>> delete(UUID personId) {
        personDAO.delete(personId);
        return Mono.fromCompletionStage(personDAO.getCount())
                .map(a -> Map.of("count", a));
    }

    @Get(uri = "/all", produces = MediaType.APPLICATION_JSON)
    public Mono<Iterable<Person>> getAll() {
        return Mono.fromCompletionStage(personDAO.getAll())
                .map(a -> a.currentPage());
    }

    @Get(uri = "/count", produces = MediaType.APPLICATION_JSON)
    public Mono<Map<String, Long>> getCount() {
        return Mono.fromCompletionStage(personDAO.getCount())
                .map(a -> Map.of("count", a));
    }

    @Post(uri = "/", produces = MediaType.APPLICATION_JSON)
    public Mono<Void> save(Person person) {
        return Mono.fromCompletionStage(personDAO.saveAsync(person));
    }
}