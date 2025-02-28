from faker import Faker


class FakeDataGenerator:
    def __init__(self):
        self.faker = Faker()
        pass

    def _generate_fake_student(self) -> dict:
        """Generate fake student data"""
        return {
            "student_id": self.faker.random_int(min=1, max=1000000),
            "first_name": self.faker.first_name(),
            "last_name": self.faker.last_name(),
            "birthdate": self.faker.date_of_birth(minimum_age=16, maximum_age=100).isoformat(),
            "year": self.faker.random_int(min=1900, max=2025),
            "contact_info": {
                "email": self.faker.email(),
                "phone": self.faker.phone_number(),
            },
            "address": {
                "street": self.faker.street_address(),
                "city": self.faker.city(),
                "state": self.faker.state(),
                "postal_code": self.faker.postcode(),
                "country": self.faker.country(),
            },
            "emergency_contacts": [self.faker.phone_number() for _ in range(3)],
            # The need to multiply by 1M, because python time.time gives the time in seconds,
            # and BigQuery Timestamp precision is microseconds.
            "last_login": int(
                self.faker.date_time_this_year(before_now=True, after_now=False).timestamp() * 1_000_000
            ),
            "metadata": self.faker.json(),
        }

    def generate_fake_students(self, n: int) -> list[dict]:
        """Generate fake students data

        Args:
            n (int): Number of fake students to generate

        Returns:
            list[dict]: List of fake students data
        """
        return [self._generate_fake_student() for _ in range(n)]

    def _generate_fake_course(self) -> dict:
        """Generate fake course data"""
        return {
            "course_id": str(self.faker.random_int(min=1, max=1000000)),
            "course_name": self.faker.word(),
            "description": self.faker.sentence(),
            "credits": str(self.faker.random_int(min=1, max=10)),
            "duration_weeks": self.faker.random_int(min=1, max=10),
            "required_materials": [self.faker.word() for _ in range(3)],
            "created_at": int(
                self.faker.date_time_this_year(before_now=True, after_now=False).timestamp() * 1_000_000
            ),
            "course_metadata": self.faker.json(),
        }

    def generate_fake_courses(self, n: int) -> list[dict]:
        """Generate fake courses data

        Args:
            n (int): Number of fake courses to generate
        """
        return [self._generate_fake_course() for _ in range(n)]
